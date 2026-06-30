use demo2::bus::TelemetrySourceKind;
use demo2::domain::{AlarmEvent, AlarmLevel};
use demo2::ingress::can::enqueue_can_tx;
use demo2::signal::{RawSample, SignalKind, SignalProcessor, SignalSample, default_signal_specs};
use demo2::transport::can::CanTxFrame;
use eframe::egui;
use egui_plot::{Line, Plot, PlotPoints, Points};
use futures_util::{TryStreamExt, pin_mut};
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_postgres::NoTls;

#[path = "collector_service.rs"]
mod embedded_collector_service;

const FEED_ADDR: &str = "127.0.0.1:19011";
const DEFAULT_PG_DSN: &str = "host=127.0.0.1 port=5432 user=postgres password=123456 dbname=demo2";
const DISPLAY_TZ_OFFSET_SECS: i64 = 8 * 3600;
const SENSOR_COUNT: usize = 10;
const WINDOW_SECS: f64 = 15.0;
const LINE_BREAK_GAP_SECS: f64 = 2.0;
const Y_RANGE_PADDING_RATIO: f64 = 0.08;
const SCALE_MIN: f32 = 0.6;
const SCALE_MAX: f32 = 2.4;
const MAX_POINTS_PER_SERIES: usize = 4096;
const UI_QUEUE_CAPACITY: usize = 50_000;
const ALARM_DB_QUEUE_CAPACITY: usize = 10_000;
const ALARM_RECORD_DEFAULT_WINDOW_MS: i64 = 5 * 60 * 1000;
const ALARM_RECORD_PAGE_SIZE: i64 = 50;
const ALARM_REPLAY_CONTEXT_MS: i64 = 30 * 1000;
const SELF_TEST_CAN_ID: u32 = 0x123;
const SELF_TEST_CAN_DLC: u8 = 8;
const SELF_TEST_CAN_DATA: [u8; 8] = [0xA5, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
const CAN_REPLAY_DEFAULT_WINDOW_MS: i64 = 5 * 60 * 1000;
const CAN_REPLAY_MIN_SPAN_SEC: f64 = 1.0;

const CAN_EXPORT_DIR: &str = "exports";


#[derive(Debug, Clone, Deserialize)]
struct CollectorConfig {
    ui_feed_addr: Option<String>,
    pg_dsn: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    collector: Option<CollectorConfig>,
}

#[derive(Clone, Debug, Deserialize)]
struct TelemetryMsg {
    pub device_id: String,
    pub sensor_id: usize,
    pub t_sec: f64,
    pub value: f64,
    pub request_id: u64,
    #[serde(default)]
    pub alarm_bit: bool,
    #[serde(default)]
    pub source_kind: TelemetrySourceKind,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
enum FeedMsg {
    Telemetry(TelemetryMsg),
    Alarm(AlarmEvent),
    Status(String),
}

#[derive(Debug)]
enum UiMsg {
    Status(String),
    Sample(TelemetryMsg),
    Alarm(AlarmEvent),
    CanReplayLoaded(ReplayMode, Result<CanReplayData, String>),
    CanReplayExported(Result<String, String>),
    AlarmRecordsLoaded(Result<AlarmRecordData, String>),
}

#[derive(Default)]
struct FeedStats {
    dropped_samples: AtomicU64,
    decode_errors: AtomicU64,
}

fn setup_chinese_fonts(ctx: &egui::Context) {
    let candidates = [
        "C:\\Windows\\Fonts\\msyh.ttc",
        "C:\\Windows\\Fonts\\msyhbd.ttc",
        "C:\\Windows\\Fonts\\simhei.ttf",
        "C:\\Windows\\Fonts\\simsun.ttc",
    ];

    for path in candidates {
        if !Path::new(path).exists() {
            continue;
        }
        let Ok(bytes) = fs::read(path) else {
            continue;
        };

        let mut fonts = egui::FontDefinitions::default();
        fonts.font_data.insert(
            "cn_font".to_owned(),
            egui::FontData::from_owned(bytes).into(),
        );
        fonts
            .families
            .entry(egui::FontFamily::Proportional)
            .or_default()
            .insert(0, "cn_font".to_owned());
        fonts
            .families
            .entry(egui::FontFamily::Monospace)
            .or_default()
            .push("cn_font".to_owned());
        ctx.set_fonts(fonts);
        break;
    }
}

fn load_feed_addr() -> String {
    if let Ok(addr) = std::env::var("DEMO2_UI_FEED_ADDR") {
        if !addr.trim().is_empty() {
            return addr;
        }
    }

    let Ok(text) = fs::read_to_string("config.toml") else {
        return FEED_ADDR.to_string();
    };

    match toml::from_str::<ConfigFile>(&text) {
        Ok(file) => file
            .collector
            .and_then(|cfg| cfg.ui_feed_addr)
            .filter(|addr| !addr.trim().is_empty())
            .unwrap_or_else(|| FEED_ADDR.to_string()),
        Err(_) => FEED_ADDR.to_string(),
    }
}

fn load_pg_dsn() -> String {
    if let Ok(dsn) = std::env::var("DEMO2_PG_DSN") {
        if !dsn.trim().is_empty() {
            return dsn;
        }
    }

    let Ok(text) = fs::read_to_string("config.toml") else {
        return DEFAULT_PG_DSN.to_string();
    };

    match toml::from_str::<ConfigFile>(&text) {
        Ok(file) => file
            .collector
            .and_then(|cfg| cfg.pg_dsn)
            .filter(|dsn| !dsn.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_PG_DSN.to_string()),
        Err(_) => DEFAULT_PG_DSN.to_string(),
    }
}

fn start_local_alarm_db_writer(pg_dsn: String) -> SyncSender<AlarmEvent> {
    let (tx, rx) = mpsc::sync_channel::<AlarmEvent>(ALARM_DB_QUEUE_CAPACITY);
    thread::spawn(move || run_local_alarm_db_writer(pg_dsn, rx));
    tx
}

fn run_local_alarm_db_writer(pg_dsn: String, rx: Receiver<AlarmEvent>) {
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("local alarm db runtime init failed: {err}");
            return;
        }
    };

    let (client, connection) = match rt.block_on(tokio_postgres::connect(&pg_dsn, NoTls)) {
        Ok(parts) => parts,
        Err(err) => {
            eprintln!("local alarm db connect failed: {err}");
            return;
        }
    };

    rt.spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("local alarm db connection failed: {err}");
        }
    });

    for alarm in rx {
        let ts_ms = alarm
            .raised_at
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_else(|_| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|duration| duration.as_millis() as i64)
                    .unwrap_or(0)
            });
        let level = format!("{:?}", alarm.level);
        let result = rt.block_on(client.execute(
            "INSERT INTO alarm_events (ts_ms, device_id, alarm_id, level, message, cleared)
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                &ts_ms,
                &alarm.device_id,
                &alarm.alarm_id,
                &level,
                &alarm.message,
                &alarm.cleared,
            ],
        ));
        if let Err(err) = result {
            eprintln!("local alarm db write failed: {err}");
        }
    }
}

fn format_datetime_input(ts_ms: i64) -> String {
    let total_seconds = ts_ms.div_euclid(1000) + DISPLAY_TZ_OFFSET_SECS;
    let days = total_seconds.div_euclid(86_400);
    let secs = total_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs / 3600;
    let minute = (secs % 3600) / 60;
    let second = secs % 60;
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}

fn format_datetime_for_filename(ts_ms: i64) -> String {
    let total_seconds = ts_ms.div_euclid(1000) + DISPLAY_TZ_OFFSET_SECS;
    let days = total_seconds.div_euclid(86_400);
    let secs = total_seconds.rem_euclid(86_400);
    let (_year, month, day) = civil_from_days(days);
    let hour = secs / 3600;
    let minute = (secs % 3600) / 60;
    let second = secs % 60;

    format!("{month:02}-{day:02}_{hour:02}-{minute:02}-{second:02}")
}

fn format_alarm_datetime(time: SystemTime) -> String {
    let ts_ms = time
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0);
    let total_seconds = ts_ms.div_euclid(1000) + DISPLAY_TZ_OFFSET_SECS;
    let days = total_seconds.div_euclid(86_400);
    let secs = total_seconds.rem_euclid(86_400);
    let (_year, month, day) = civil_from_days(days);
    let hour = secs / 3600;
    let minute = (secs % 3600) / 60;
    let second = secs % 60;
    format!("{month:02}/{day:02} {hour:02}:{minute:02}:{second:02}")
}

fn default_can_export_filename(start_ts_ms: i64, end_ts_ms: i64, now_ts_ms: i64) -> String {
    format!(
        "can_export_{}_{}_{}.txt",        
        format_datetime_for_filename(now_ts_ms),
        format_datetime_for_filename(start_ts_ms),
        format_datetime_for_filename(end_ts_ms)
    )
}

fn parse_time_input(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(ts_ms) = trimmed.parse::<i64>() {
        return Some(ts_ms);
    }

    let normalized = trimmed.replace('/', "-").replace('T', " ");
    let mut parts = normalized.split_whitespace();
    let date_part = parts.next()?;
    let time_part = parts.next().unwrap_or("00:00:00");
    if parts.next().is_some() {
        return None;
    }

    let mut date = date_part.split('-');
    let year = date.next()?.parse::<i64>().ok()?;
    let month = date.next()?.parse::<i64>().ok()?;
    let day = date.next()?.parse::<i64>().ok()?;
    if date.next().is_some() {
        return None;
    }

    let mut time = time_part.split(':');
    let hour = time.next()?.parse::<i64>().ok()?;
    let minute = time.next().unwrap_or("0").parse::<i64>().ok()?;
    let second = time.next().unwrap_or("0").parse::<i64>().ok()?;
    if time.next().is_some() {
        return None;
    }

    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=59).contains(&second)
    {
        return None;
    }

    let days = days_from_civil(year, month, day);
    let local_seconds = days * 86_400 + hour * 3600 + minute * 60 + second;
    Some((local_seconds - DISPLAY_TZ_OFFSET_SECS) * 1000)
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let y = y + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

#[allow(dead_code)]
fn feed_thread(feed_addr: String, tx: SyncSender<UiMsg>, stats: Arc<FeedStats>) {
    let stream = match TcpStream::connect(&feed_addr) {
        Ok(s) => s,
        Err(err) => {
            let _ = tx.try_send(UiMsg::Status(format!("连接失败: {err}")));
            return;
        }
    };

    let _ = tx.try_send(UiMsg::Status(format!(
        "连接到: {feed_addr}"
    )));
    let reader = BufReader::new(stream);

    for line in reader.lines() {
        let Ok(line) = line else {
            let _ = tx.try_send(UiMsg::Status("feed closed".to_string()));
            break;
        };

        match serde_json::from_str::<FeedMsg>(&line) {
            Ok(FeedMsg::Telemetry(msg)) => match tx.try_send(UiMsg::Sample(msg)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    stats.dropped_samples.fetch_add(1, Ordering::Relaxed);
                }
                Err(TrySendError::Disconnected(_)) => return,
            },
            Ok(FeedMsg::Alarm(alarm)) => match tx.try_send(UiMsg::Alarm(alarm)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    stats.dropped_samples.fetch_add(1, Ordering::Relaxed);
                }
                Err(TrySendError::Disconnected(_)) => return,
            },
            Ok(FeedMsg::Status(status)) => match tx.try_send(UiMsg::Status(status)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    stats.dropped_samples.fetch_add(1, Ordering::Relaxed);
                }
                Err(TrySendError::Disconnected(_)) => return,
            },
            Err(_) => match serde_json::from_str::<TelemetryMsg>(&line) {
                Ok(msg) => match tx.try_send(UiMsg::Sample(msg)) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        stats.dropped_samples.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(TrySendError::Disconnected(_)) => return,
                },
                Err(_) => {
                    stats.decode_errors.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.try_send(UiMsg::Status("feed decode error".to_string()));
                }
            },
        }
    }
}

fn resilient_feed_thread(feed_addr: String, tx: SyncSender<UiMsg>, stats: Arc<FeedStats>) {
    loop {
        let stream = match TcpStream::connect(&feed_addr) {
            Ok(s) => s,
            Err(err) => {
                let _ = tx.try_send(UiMsg::Status(format!("等待 collector 中: {err}")));
                thread::sleep(Duration::from_millis(500));
                continue;
            }
        };

        let _ = tx.try_send(UiMsg::Status(format!("已连接 collector feed: {feed_addr}")));
        let reader = BufReader::new(stream);

        for line in reader.lines() {
            let Ok(line) = line else {
                let _ = tx.try_send(UiMsg::Status("collector feed 已断开，正在重连".to_string()));
                break;
            };

            match serde_json::from_str::<FeedMsg>(&line) {
                Ok(FeedMsg::Telemetry(msg)) => match tx.try_send(UiMsg::Sample(msg)) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        stats.dropped_samples.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(TrySendError::Disconnected(_)) => return,
                },
                Ok(FeedMsg::Alarm(alarm)) => match tx.try_send(UiMsg::Alarm(alarm)) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        stats.dropped_samples.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(TrySendError::Disconnected(_)) => return,
                },
                Ok(FeedMsg::Status(status)) => match tx.try_send(UiMsg::Status(status)) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        stats.dropped_samples.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(TrySendError::Disconnected(_)) => return,
                },
                Err(_) => match serde_json::from_str::<TelemetryMsg>(&line) {
                    Ok(msg) => match tx.try_send(UiMsg::Sample(msg)) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => {
                            stats.dropped_samples.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(TrySendError::Disconnected(_)) => return,
                    },
                    Err(_) => {
                        stats.decode_errors.fetch_add(1, Ordering::Relaxed);
                        let _ = tx.try_send(UiMsg::Status("feed decode error".to_string()));
                    }
                },
            }
        }

        thread::sleep(Duration::from_millis(300));
    }
}

struct SensorSeries {
    points: VecDeque<[f64; 2]>,
    latest: Option<f64>,
    device_id: String,
}

impl SensorSeries {
    fn new() -> Self {
        Self {
            points: VecDeque::with_capacity(1024),
            latest: None,
            device_id: String::new(),
        }
    }

    fn push(&mut self, msg: &TelemetryMsg) {
        self.points.push_back([msg.t_sec, msg.value]);
        self.latest = Some(msg.value);
        self.device_id = msg.device_id.clone();

        let min_t = msg.t_sec - WINDOW_SECS;
        while let Some(front) = self.points.front() {
            if front[0] < min_t {
                let _ = self.points.pop_front();
            } else {
                break;
            }
        }
        while self.points.len() > MAX_POINTS_PER_SERIES {
            let _ = self.points.pop_front();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestSignalView {
    Demo,
    Sent,
    CanFrame,
    TcpFrame,
}

#[derive(Clone, Copy, Debug)]
enum SignalBinding {
    DemoAxisX,
    DemoAxisY,
    DemoAxisZ,
    Sent1V1,
    Sent1P1,
    Sent2V2,
    Sent2P2,
    Sent3Angle,
    CanAxisX,
    CanAxisY,
    CanAxisZ,
    TcpSensor(usize),
}

impl SignalBinding {
    fn title(self, index: u32) -> String {
        match self {
            Self::DemoAxisX => format!("demo_x_{index}"),
            Self::DemoAxisY => format!("demo_y_{index}"),
            Self::DemoAxisZ => format!("demo_z_{index}"),
            Self::Sent1V1 => format!("sent_t1_angle_{index}"),
            Self::Sent1P1 => format!("sent_t1_torque_{index}"),
            Self::Sent2V2 => format!("sent_t2_angle_{index}"),
            Self::Sent2P2 => format!("sent_t2_torque_{index}"),
            Self::Sent3Angle => format!("sent_s_angle_{index}"),
            Self::CanAxisX => format!("can_x_{index}"),
            Self::CanAxisY => format!("can_y_{index}"),
            Self::CanAxisZ => format!("can_z_{index}"),
            Self::TcpSensor(sensor_id) => format!("tcp_sensor_{sensor_id}_{index}"),
        }
    }

    fn sensor_id(self) -> usize {
        match self {
            Self::DemoAxisX => 0,
            Self::DemoAxisY => 1,
            Self::DemoAxisZ => 2,
            Self::Sent1V1 => 0,
            Self::Sent1P1 => 1,
            Self::Sent2V2 => 2,
            Self::Sent2P2 => 3,
            Self::Sent3Angle => 4,
            Self::CanAxisX => 0,
            Self::CanAxisY => 1,
            Self::CanAxisZ => 2,
            Self::TcpSensor(sensor_id) => sensor_id,
        }
    }

    fn uses_tcp_series(self) -> bool {
        matches!(self, Self::TcpSensor(_))
    }

    fn chart_label(self) -> Option<&'static str> {
        match self {
            Self::DemoAxisX => Some("demo axis X"),
            Self::DemoAxisY => Some("demo axis Y"),
            Self::DemoAxisZ => Some("demo axis Z"),
            Self::Sent1V1 => Some("SENT T1 angle"),
            Self::Sent1P1 => Some("SENT T1 torque"),
            Self::Sent2V2 => Some("SENT T2 angle"),
            Self::Sent2P2 => Some("SENT T2 torque"),
            Self::Sent3Angle => Some("SENT S angle"),
            Self::CanAxisX => Some("CAN axis X"),
            Self::CanAxisY => Some("CAN axis Y"),
            Self::CanAxisZ => Some("CAN axis Z"),
            _ => None,
        }
    }

    fn is_can_axis(self) -> bool {
        matches!(self, Self::CanAxisX | Self::CanAxisY | Self::CanAxisZ)
    }

    fn value_label(self) -> &'static str {
        match self {
            Self::Sent1V1 | Self::Sent2V2 | Self::Sent3Angle => "angle",
            Self::Sent1P1 | Self::Sent2P2 => "torque",
            _ => "raw",
        }
    }

    fn demo_derived_angle_id(self) -> Option<&'static str> {
        match self {
            Self::DemoAxisX => Some("sensor_0_angle"),
            Self::DemoAxisY => Some("sensor_1_angle"),
            Self::DemoAxisZ => Some("sensor_2_angle"),
            _ => None,
        }
    }
}

struct DynamicSignalWindow {
    title: String,
    binding: Option<SignalBinding>,
    position: egui::Pos2,
    scale: f32,
    rect: Option<egui::Rect>,
}

struct AlarmViewItem {
    event: AlarmEvent,
    received_at: Instant,
}

#[derive(Clone, Copy, Debug, Default)]
struct CanAxisAlarmState {
    high_active: bool,
    low_active: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SentJumpLevel {
    Normal,
    Warning,
    Critical,
    Purple,
}

#[derive(Clone, Copy, Debug)]
struct SentTorqueJumpState {
    last_value: Option<f64>,
    last_delta: f64,
    alert_count: u64,
    level: SentJumpLevel,
}

impl Default for SentTorqueJumpState {
    fn default() -> Self {
        Self {
            last_value: None,
            last_delta: 0.0,
            alert_count: 0,
            level: SentJumpLevel::Normal,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct SentAngleJumpState {
    last_value: Option<f64>,
    last_delta: f64,
    alert_count: u64,
    active: bool,
}

impl Default for SentAngleJumpState {
    fn default() -> Self {
        Self {
            last_value: None,
            last_delta: 0.0,
            alert_count: 0,
            active: false,
        }
    }
}

#[derive(Clone, Debug)]
struct CanAlarmThresholds {
    x_high_input: String,
    x_low_input: String,
    y_high_input: String,
    y_low_input: String,
    z_high_input: String,
    z_low_input: String,
    x_high_applied: String,
    x_low_applied: String,
    y_high_applied: String,
    y_low_applied: String,
    z_high_applied: String,
    z_low_applied: String,
}

impl Default for CanAlarmThresholds {
    fn default() -> Self {
        Self {
            x_high_input: String::new(),
            x_low_input: String::new(),
            y_high_input: String::new(),
            y_low_input: String::new(),
            z_high_input: String::new(),
            z_low_input: String::new(),
            x_high_applied: String::new(),
            x_low_applied: String::new(),
            y_high_applied: String::new(),
            y_low_applied: String::new(),
            z_high_applied: String::new(),
            z_low_applied: String::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct SentTorqueJumpThresholds {
    warn_input: String,
    red_input: String,
    purple_input: String,
    warn_applied: String,
    red_applied: String,
    purple_applied: String,
}

impl Default for SentTorqueJumpThresholds {
    fn default() -> Self {
        Self {
            warn_input: "0.2".to_string(),
            red_input: "0.3".to_string(),
            purple_input: "0.4".to_string(),
            warn_applied: "0.2".to_string(),
            red_applied: "0.3".to_string(),
            purple_applied: "0.4".to_string(),
        }
    }
}

#[derive(Clone, Debug)]
struct SentAngleJumpThresholds {
    red_input: String,
    red_applied: String,
}

impl Default for SentAngleJumpThresholds {
    fn default() -> Self {
        Self {
            red_input: "1.0".to_string(),
            red_applied: "1.0".to_string(),
        }
    }
}

#[derive(Clone, Debug)]
struct AlarmPlotPoint {
    point: [f64; 2],
    level: String,
    cleared: bool,
}

fn nearest_plot_point(points: &[[f64; 2]], x_sec: f64) -> Option<[f64; 2]> {
    points
        .iter()
        .min_by(|a, b| {
            (a[0] - x_sec)
                .abs()
                .partial_cmp(&(b[0] - x_sec).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .copied()
}

#[derive(Clone, Debug, Default)]
struct CanReplayData {
    x_points: Vec<[f64; 2]>,
    y_points: Vec<[f64; 2]>,
    z_points: Vec<[f64; 2]>,
    u_points: Vec<[f64; 2]>,
    v_points: Vec<[f64; 2]>,
    x_alarm_points: Vec<AlarmPlotPoint>,
    y_alarm_points: Vec<AlarmPlotPoint>,
    z_alarm_points: Vec<AlarmPlotPoint>,
    u_alarm_points: Vec<AlarmPlotPoint>,
    v_alarm_points: Vec<AlarmPlotPoint>,
    min_ts_ms: i64,
    max_ts_ms: i64,
}

impl CanReplayData {
    fn is_empty(&self) -> bool {
        self.x_points.is_empty()
            && self.y_points.is_empty()
            && self.z_points.is_empty()
            && self.u_points.is_empty()
            && self.v_points.is_empty()
    }

    fn total_span_sec(&self) -> f64 {
        ((self.max_ts_ms - self.min_ts_ms).max(0) as f64) / 1000.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplayMode {
    Can3Axis,
    Sent,
}

impl ReplayMode {
    fn load_status(self) -> &'static str {
        match self {
            Self::Can3Axis => "正在加载 3轴 回放数据...",
            Self::Sent => "正在加载 SENT 回放数据...",
        }
    }

    fn empty_status(self) -> &'static str {
        match self {
            Self::Can3Axis => "该时间段没有 3轴 数据",
            Self::Sent => "该时间段没有 SENT 数据",
        }
    }

    fn export_header(self) -> &'static str {
        match self {
            Self::Can3Axis => "# CAN 3-axis export",
            Self::Sent => "# SENT export",
        }
    }

    fn series_labels(self) -> [&'static str; 5] {
        match self {
            Self::Can3Axis => ["X", "Y", "Z", "", ""],
            Self::Sent => ["T1 angle", "T1 torque", "T2 angle", "T2 torque", "S angle"],
        }
    }

    fn axis_filters(self) -> [&'static str; 5] {
        match self {
            Self::Can3Axis => ["x", "y", "z", "__unused_4__", "__unused_5__"],
            Self::Sent => ["t1_angle", "t1_torque", "t2_angle", "t2_torque", "s_angle"],
        }
    }
}

struct CanReplayState {
    open: bool,
    loading: bool,
    exporting: bool,
    status: String,
    pg_dsn: String,
    start_ts_input: String,
    end_ts_input: String,
    mode: ReplayMode,
    show_x: bool,
    show_y: bool,
    show_z: bool,
    show_u: bool,
    show_v: bool,
    show_alarm_points: bool,
    data: Option<CanReplayData>,
    view_start_sec: f64,
    view_span_sec: f64,
    plot_rect: Option<egui::Rect>,
}

impl CanReplayState {
    fn new(pg_dsn: String) -> Self {
        Self {
            open: false,
            loading: false,
            exporting: false,
            status: "未加载".to_string(),
            pg_dsn,
            start_ts_input: String::new(),
            end_ts_input: String::new(),
            mode: ReplayMode::Can3Axis,
            show_x: true,
            show_y: true,
            show_z: true,
            show_u: true,
            show_v: true,
            show_alarm_points: true,
            data: None,
            view_start_sec: 0.0,
            view_span_sec: 60.0,
            plot_rect: None,
        }
    }
}

#[derive(Clone, Debug)]
struct AlarmRecordRow {
    ts_ms: i64,
    device_id: String,
    alarm_id: String,
    level: String,
    message: String,
    cleared: bool,
}

#[derive(Clone, Debug, Default)]
struct AlarmRecordData {
    records: Vec<AlarmRecordRow>,
    page_index: i64,
    has_next: bool,
    total_count: i64,
}

impl AlarmRecordData {
    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

struct AlarmRecordState {
    open: bool,
    loading: bool,
    status: String,
    pg_dsn: String,
    start_ts_input: String,
    end_ts_input: String,
    show_can_x: bool,
    show_can_y: bool,
    show_can_z: bool,
    show_sent_t1: bool,
    show_sent_t2: bool,
    show_sent_s: bool,
    page_index: i64,
    has_next: bool,
    data: Option<AlarmRecordData>,
}

impl AlarmRecordState {
    fn new(pg_dsn: String) -> Self {
        Self {
            open: false,
            loading: false,
            status: "未加载".to_string(),
            pg_dsn,
            start_ts_input: String::new(),
            end_ts_input: String::new(),
            show_can_x: true,
            show_can_y: true,
            show_can_z: true,
            show_sent_t1: true,
            show_sent_t2: true,
            show_sent_s: true,
            page_index: 0,
            has_next: false,
            data: None,
        }
    }
}

struct UiClientApp {
    rx: Receiver<UiMsg>,
    ui_tx: SyncSender<UiMsg>,
    local_alarm_db_tx: SyncSender<AlarmEvent>,
    feed_stats: Arc<FeedStats>,
    feed_addr: String,
    status: String,
    sensors: Vec<SensorSeries>,
    tcp_sensors: Vec<SensorSeries>,
    total_samples: u64,
    last_req: u64,
    can_self_test_counter: u8,
    signal_processor: SignalProcessor,
    derived_signals: HashMap<(String, String), SensorSeries>,
    selected_view: TestSignalView,
    dynamic_windows: Vec<DynamicSignalWindow>,
    demo_alarm_bit_state: Option<bool>,
    demo_group_index: u32,
    sent1_group_index: u32,
    can_group_index: u32,
    tcp_group_index: u32,
    active_alarms: HashMap<String, AlarmViewItem>,
    alarm_history: VecDeque<AlarmViewItem>,
    total_alarm_count: u64,
    can_alarm_thresholds: CanAlarmThresholds,
    can_alarm_states: HashMap<(String, usize), CanAxisAlarmState>,
    sent_jump_thresholds: SentTorqueJumpThresholds,
    sent_jump_states: HashMap<(String, usize), SentTorqueJumpState>,
    sent_angle_jump_thresholds: SentAngleJumpThresholds,
    sent_angle_jump_states: HashMap<(String, usize), SentAngleJumpState>,
    last_can_self_test_result: String,
    can_replay: CanReplayState,
    alarm_records: AlarmRecordState,
}

impl UiClientApp {
    const MAX_ALARM_HISTORY: usize = 50;
    const DEMO_ALARM_ID: &'static str = "demo_alarm_bit";

    fn default_window_pos(index: usize) -> egui::Pos2 {
        let col = (index % 4) as f32;
        let row = (index / 4) as f32;
        egui::pos2(180.0 + col * (300.0 + 20.0), 220.0 + row * (200.0 + 50.0))
    }

    fn new(
        rx: Receiver<UiMsg>,
        ui_tx: SyncSender<UiMsg>,
        feed_stats: Arc<FeedStats>,
        feed_addr: String,
        pg_dsn: String,
    ) -> Self {
        let local_alarm_db_tx = start_local_alarm_db_writer(pg_dsn.clone());
        Self {
            rx,
            ui_tx,
            local_alarm_db_tx,
            feed_stats,
            feed_addr,
            status: "starting...".to_string(),
            sensors: (0..SENSOR_COUNT).map(|_| SensorSeries::new()).collect(),
            tcp_sensors: (0..SENSOR_COUNT).map(|_| SensorSeries::new()).collect(),
            total_samples: 0,
            last_req: 0,
            can_self_test_counter: 0,
            signal_processor: SignalProcessor::new(default_signal_specs(SENSOR_COUNT)),
            derived_signals: HashMap::new(),
            selected_view: TestSignalView::Sent,
            dynamic_windows: Vec::new(),
            demo_alarm_bit_state: None,
            demo_group_index: 1,
            sent1_group_index: 1,
            can_group_index: 1,
            tcp_group_index: 1,
            active_alarms: HashMap::new(),
            alarm_history: VecDeque::with_capacity(Self::MAX_ALARM_HISTORY),
            total_alarm_count: 0,
            can_alarm_thresholds: CanAlarmThresholds::default(),
            can_alarm_states: HashMap::new(),
            sent_jump_thresholds: SentTorqueJumpThresholds::default(),
            sent_jump_states: HashMap::new(),
            sent_angle_jump_thresholds: SentAngleJumpThresholds::default(),
            sent_angle_jump_states: HashMap::new(),
            last_can_self_test_result: "未执行".to_string(),
            can_replay: CanReplayState::new(pg_dsn.clone()),
            alarm_records: AlarmRecordState::new(pg_dsn),
        }
    }

    fn reset_layout(&mut self) {
        for (idx, window) in self.dynamic_windows.iter_mut().enumerate() {
            window.position = Self::default_window_pos(idx);
            window.scale = 1.0;
            window.rect = None;
        }
    }

    fn detect_view_for_sample(sample: &TelemetryMsg) -> Option<TestSignalView> {
        match sample.source_kind {
            TelemetrySourceKind::SerialDemo => Some(TestSignalView::Demo),
            TelemetrySourceKind::SerialSent1
            | TelemetrySourceKind::SerialSent2
            | TelemetrySourceKind::SerialSent3
            | TelemetrySourceKind::CanSent => Some(TestSignalView::Sent),
            TelemetrySourceKind::CanAxis => Some(TestSignalView::CanFrame),
            TelemetrySourceKind::TcpFrame => Some(TestSignalView::TcpFrame),
            TelemetrySourceKind::Unknown | TelemetrySourceKind::FrameStream => {
                if sample.device_id.starts_with("can://") {
                    return Some(TestSignalView::CanFrame);
                }
                if sample.device_id.starts_with("tcp://") {
                    return Some(TestSignalView::TcpFrame);
                }

                match sample.sensor_id {
                    0..=4 => Some(TestSignalView::Sent),
                    _ => None,
                }
            }
        }
    }

    fn switch_to_view(&mut self, view: TestSignalView) {
        self.selected_view = view;
        self.dynamic_windows.clear();
        self.add_dynamic_window();
        self.reset_layout();
    }

    fn drain_messages(&mut self) {
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                UiMsg::Status(s) => {
                    if s.contains("CAN 自检结果") {
                        self.last_can_self_test_result = s.clone();
                    } else if s.contains("CAN self-test") || s.contains("CAN 自检") {
                        self.last_can_self_test_result = s.clone();
                    }
                    self.status = s;
                }
                UiMsg::Alarm(alarm) => self.apply_alarm(alarm),
                UiMsg::CanReplayLoaded(mode, result) => {
                    self.can_replay.loading = false;
                    if mode != self.can_replay.mode {
                        continue;
                    }
                    match result {
                        Ok(data) => {
                            let span_sec = data.total_span_sec();
                            self.can_replay.view_start_sec = 0.0;
                            self.can_replay.view_span_sec =
                                span_sec.clamp(CAN_REPLAY_MIN_SPAN_SEC, 120.0);
                            self.can_replay.status = if data.is_empty() {
                                self.can_replay.mode.empty_status().to_string()
                            } else {
                                format!(
                                    "已加载 {} 条回放点位",
                                    data.x_points.len()
                                        + data.y_points.len()
                                        + data.z_points.len()
                                        + data.u_points.len()
                                        + data.v_points.len()
                                )
                            };
                            self.can_replay.data = Some(data);
                        }
                        Err(err) => {
                            self.can_replay.status = format!("加载失败: {err}");
                            self.can_replay.data = None;
                        }
                    }
                }
                UiMsg::CanReplayExported(result) => {
                    self.can_replay.exporting = false;
                    self.can_replay.status = match result {
                        Ok(path) => format!("导出完成: {path}"),
                        Err(err) => format!("导出失败: {err}"),
                    };
                }
                UiMsg::AlarmRecordsLoaded(result) => {
                    self.alarm_records.loading = false;
                    match result {
                        Ok(data) => {
                            let count = data.records.len();
                            self.alarm_records.page_index = data.page_index;
                            self.alarm_records.has_next = data.has_next;
                            self.alarm_records.status = if data.is_empty() {
                                format!("第 {} 页没有报警记录", data.page_index + 1)
                            } else {
                                format!("第 {} 页，{} 条报警记录", data.page_index + 1, count)
                            };
                            self.alarm_records.status = if data.is_empty() {
                                format!(
                                    "第 {} 页没有报警记录，总报警记录 {} 条",
                                    data.page_index + 1,
                                    data.total_count
                                )
                            } else {
                                format!(
                                    "第 {} 页，{} 条报警记录，总报警记录 {} 条",
                                    data.page_index + 1,
                                    count,
                                    data.total_count
                                )
                            };
                            self.alarm_records.data = Some(data);
                        }
                        Err(err) => {
                            self.alarm_records.status = format!("加载失败: {err}");
                            self.alarm_records.data = None;
                        }
                    }
                }

                
                UiMsg::Sample(sample) => {
                    if sample.source_kind == TelemetrySourceKind::SerialDemo {
                        self.demo_alarm_bit_state = Some(sample.alarm_bit);
                    }
                    if let Some(view) = Self::detect_view_for_sample(&sample) {
                        let should_switch =
                            self.selected_view != view || self.dynamic_windows.is_empty();
                        if should_switch {
                            self.switch_to_view(view);
                        }
                    }
                    if sample.sensor_id < SENSOR_COUNT {
                        if Self::is_sent_torque_sample(&sample) {
                            self.evaluate_sent_torque_jump(
                                &sample.device_id,
                                sample.sensor_id,
                                sample.value,
                            );
                        }
                        if Self::is_sent_angle_sample(&sample) {
                            self.evaluate_sent_angle_jump(
                                &sample.device_id,
                                sample.sensor_id,
                                sample.value,
                            );
                        }
                        let processed = self.signal_processor.ingest_raw(RawSample {
                            device_id: sample.device_id.clone(),
                            sensor_id: sample.sensor_id,
                            t_sec: sample.t_sec,
                            value: sample.value,
                            req_id: sample.request_id,
                        });
                        for signal in processed {
                            self.apply_signal_sample(signal);
                        }
                        self.total_samples = self.total_samples.saturating_add(1);
                        self.last_req = sample.request_id;
                    }
                }
            }
        }
    }

    fn alarm_key(event: &AlarmEvent) -> String {
        format!("{}::{}", event.device_id, event.alarm_id)
    }

    fn apply_alarm(&mut self, alarm: AlarmEvent) {
        let key = Self::alarm_key(&alarm);
        let received_at = Instant::now();
        self.total_alarm_count = self.total_alarm_count.saturating_add(1);

        if alarm.cleared {
            self.active_alarms.remove(&key);
        } else {
            self.active_alarms.insert(
                key,
                AlarmViewItem {
                    event: alarm.clone(),
                    received_at,
                },
            );
        }

        self.alarm_history.push_front(AlarmViewItem {
            event: alarm,
            received_at,
        });
        while self.alarm_history.len() > Self::MAX_ALARM_HISTORY {
            self.alarm_history.pop_back();
        }
    }

    fn apply_local_alarm(&mut self, alarm: AlarmEvent) {
        match self.local_alarm_db_tx.try_send(alarm.clone()) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.status = "local alarm db queue full, dropped alarm db write".to_string();
            }
            Err(TrySendError::Disconnected(_)) => {
                self.status = "local alarm db writer disconnected".to_string();
            }
        }
        self.apply_alarm(alarm);
    }

    fn parse_optional_threshold(text: &str) -> Option<f64> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            None
        } else {
            trimmed.parse::<f64>().ok()
        }
    }

    fn validate_threshold_text(text: &str) -> Result<String, String> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Ok(String::new());
        }
        trimmed
            .parse::<f64>()
            .map(|_| trimmed.to_string())
            .map_err(|_| format!("无效阈值: {trimmed}"))
    }

    fn apply_can_alarm_thresholds(&mut self) -> Result<(), String> {
        let x_high = Self::validate_threshold_text(&self.can_alarm_thresholds.x_high_input)?;
        let x_low = Self::validate_threshold_text(&self.can_alarm_thresholds.x_low_input)?;
        let y_high = Self::validate_threshold_text(&self.can_alarm_thresholds.y_high_input)?;
        let y_low = Self::validate_threshold_text(&self.can_alarm_thresholds.y_low_input)?;
        let z_high = Self::validate_threshold_text(&self.can_alarm_thresholds.z_high_input)?;
        let z_low = Self::validate_threshold_text(&self.can_alarm_thresholds.z_low_input)?;

        self.can_alarm_thresholds.x_high_applied = x_high;
        self.can_alarm_thresholds.x_low_applied = x_low;
        self.can_alarm_thresholds.y_high_applied = y_high;
        self.can_alarm_thresholds.y_low_applied = y_low;
        self.can_alarm_thresholds.z_high_applied = z_high;
        self.can_alarm_thresholds.z_low_applied = z_low;
        Ok(())
    }

    fn can_alarm_rule(&self, sensor_id: usize) -> (Option<f64>, Option<f64>, &'static str) {
        match sensor_id {
            0 => (
                Self::parse_optional_threshold(&self.can_alarm_thresholds.x_high_applied),
                Self::parse_optional_threshold(&self.can_alarm_thresholds.x_low_applied),
                "x",
            ),
            1 => (
                Self::parse_optional_threshold(&self.can_alarm_thresholds.y_high_applied),
                Self::parse_optional_threshold(&self.can_alarm_thresholds.y_low_applied),
                "y",
            ),
            2 => (
                Self::parse_optional_threshold(&self.can_alarm_thresholds.z_high_applied),
                Self::parse_optional_threshold(&self.can_alarm_thresholds.z_low_applied),
                "z",
            ),
            _ => (None, None, ""),
        }
    }

    fn can_chart_thresholds(&self, binding: SignalBinding) -> Option<(Option<f64>, Option<f64>)> {
        if !binding.is_can_axis() {
            return None;
        }
        let (high, low, _) = self.can_alarm_rule(binding.sensor_id());
        Some((high, low))
    }

    fn emit_can_threshold_alarm(
        &mut self,
        device_id: &str,
        axis: &str,
        bound: &str,
        threshold: f64,
        value: f64,
        cleared: bool,
    ) {
        self.apply_local_alarm(AlarmEvent {
            device_id: device_id.to_string(),
            alarm_id: format!("can_{axis}_{bound}"),
            level: AlarmLevel::Warning,
            message: format!(
                "axis={axis}, bound={bound}, threshold={threshold:.3}, value={value:.3}"
            ),
            raised_at: std::time::SystemTime::now(),
            cleared,
        });
    }

    fn evaluate_can_threshold_alarms(&mut self, device_id: &str, sensor_id: usize, value: f64) {
        if !device_id.starts_with("can://") || sensor_id > 2 {
            return;
        }

        let (high, low, axis) = self.can_alarm_rule(sensor_id);
        if axis.is_empty() {
            return;
        }

        let key = (device_id.to_string(), sensor_id);
        let mut state = self.can_alarm_states.get(&key).copied().unwrap_or_default();

        match high {
            Some(threshold) => {
                if !state.high_active && value >= threshold {
                    state.high_active = true;
                    self.emit_can_threshold_alarm(device_id, axis, "h", threshold, value, false);
                } else if state.high_active && value < threshold {
                    state.high_active = false;
                    self.emit_can_threshold_alarm(device_id, axis, "h", threshold, value, true);
                }
            }
            None => {
                if state.high_active {
                    self.emit_can_threshold_alarm(device_id, axis, "h", value, value, true);
                }
                state.high_active = false;
            }
        }

        match low {
            Some(threshold) => {
                if !state.low_active && value <= threshold {
                    state.low_active = true;
                    self.emit_can_threshold_alarm(device_id, axis, "l", threshold, value, false);
                } else if state.low_active && value > threshold {
                    state.low_active = false;
                    self.emit_can_threshold_alarm(device_id, axis, "l", threshold, value, true);
                }
            }
            None => {
                if state.low_active {
                    self.emit_can_threshold_alarm(device_id, axis, "l", value, value, true);
                }
                state.low_active = false;
            }
        }

        self.can_alarm_states.insert(key, state);
    }

    fn is_sent_torque_sample(sample: &TelemetryMsg) -> bool {
        matches!(
            sample.source_kind,
            TelemetrySourceKind::CanSent
                | TelemetrySourceKind::SerialSent1
                | TelemetrySourceKind::SerialSent2
        ) && matches!(sample.sensor_id, 1 | 3)
    }

    fn is_sent_angle_sample(sample: &TelemetryMsg) -> bool {
        matches!(
            sample.source_kind,
            TelemetrySourceKind::CanSent
                | TelemetrySourceKind::SerialSent1
                | TelemetrySourceKind::SerialSent2
        ) && matches!(sample.sensor_id, 0 | 2 | 4)
    }

    fn sent_torque_label(sensor_id: usize) -> &'static str {
        match sensor_id {
            1 => "T1 torque",
            3 => "T2 torque",
            _ => "SENT torque",
        }
    }

    fn sent_angle_label(sensor_id: usize) -> &'static str {
        match sensor_id {
            0 => "T1 angle",
            2 => "T2 angle",
            4 => "S angle",
            _ => "SENT angle",
        }
    }

    fn sent_jump_thresholds(&self) -> (f64, f64, f64) {
        let warn = Self::parse_optional_threshold(&self.sent_jump_thresholds.warn_applied)
            .unwrap_or(0.2)
            .abs();
        let red = Self::parse_optional_threshold(&self.sent_jump_thresholds.red_applied)
            .unwrap_or(0.3)
            .abs()
            .max(warn);
        let purple = Self::parse_optional_threshold(&self.sent_jump_thresholds.purple_applied)
            .unwrap_or(0.4)
            .abs()
            .max(red);
        (warn, red, purple)
    }

    fn apply_sent_jump_thresholds(&mut self) -> Result<(), String> {
        let warn = Self::validate_threshold_text(&self.sent_jump_thresholds.warn_input)?;
        let red = Self::validate_threshold_text(&self.sent_jump_thresholds.red_input)?;
        let purple = Self::validate_threshold_text(&self.sent_jump_thresholds.purple_input)?;
        let warn_value = Self::parse_optional_threshold(&warn).unwrap_or(0.2).abs();
        let red_value = Self::parse_optional_threshold(&red).unwrap_or(0.3).abs();
        let purple_value = Self::parse_optional_threshold(&purple).unwrap_or(0.4).abs();
        if red_value < warn_value {
            return Err("red threshold must be >= warning threshold".to_string());
        }
        if purple_value < red_value {
            return Err("purple threshold must be >= red threshold".to_string());
        }
        self.sent_jump_thresholds.warn_applied = format!("{warn_value:.3}");
        self.sent_jump_thresholds.red_applied = format!("{red_value:.3}");
        self.sent_jump_thresholds.purple_applied = format!("{purple_value:.3}");
        Ok(())
    }

    fn sent_jump_level(&self, delta_abs: f64) -> SentJumpLevel {
        let (warn, red, purple) = self.sent_jump_thresholds();
        if delta_abs >= purple {
            SentJumpLevel::Purple
        } else if delta_abs >= red {
            SentJumpLevel::Critical
        } else if delta_abs > warn {
            SentJumpLevel::Warning
        } else {
            SentJumpLevel::Normal
        }
    }

    fn emit_sent_jump_alarm(
        &mut self,
        device_id: &str,
        sensor_id: usize,
        delta_abs: f64,
        level: SentJumpLevel,
        cleared: bool,
    ) {
        let alarm_level = match level {
            SentJumpLevel::Purple => AlarmLevel::Purple,
            SentJumpLevel::Critical => AlarmLevel::Critical,
            _ => AlarmLevel::Warning,
        };
        let (warn, red, purple) = self.sent_jump_thresholds();
        self.apply_local_alarm(AlarmEvent {
            device_id: device_id.to_string(),
            alarm_id: format!("sent_torque_jump_{}", if sensor_id == 1 { "t1" } else { "t2" }),
            level: alarm_level,
            message: format!(
                "{} jump={delta_abs:.3}, warn={warn:.3}, red={red:.3}, purple={purple:.3}",
                Self::sent_torque_label(sensor_id)
            ),
            raised_at: std::time::SystemTime::now(),
            cleared,
        });
    }

    fn evaluate_sent_torque_jump(&mut self, device_id: &str, sensor_id: usize, value: f64) {
        let key = (device_id.to_string(), sensor_id);
        let previous = self
            .sent_jump_states
            .get(&key)
            .copied()
            .unwrap_or_default();
        let Some(last_value) = previous.last_value else {
            self.sent_jump_states.insert(
                key,
                SentTorqueJumpState {
                    last_value: Some(value),
                    ..previous
                },
            );
            return;
        };

        let delta_abs = (value - last_value).abs();
        let next_level = self.sent_jump_level(delta_abs);
        let mut next = previous;
        next.last_value = Some(value);
        next.last_delta = delta_abs;

        if next_level != previous.level {
            if previous.level != SentJumpLevel::Normal {
                self.emit_sent_jump_alarm(device_id, sensor_id, delta_abs, previous.level, true);
            }
            if next_level != SentJumpLevel::Normal {
                next.alert_count = next.alert_count.saturating_add(1);
                self.emit_sent_jump_alarm(device_id, sensor_id, delta_abs, next_level, false);
            }
        } else if next_level == SentJumpLevel::Critical && previous.level == SentJumpLevel::Warning
        {
            self.emit_sent_jump_alarm(device_id, sensor_id, delta_abs, next_level, false);
        }

        next.level = next_level;
        self.sent_jump_states.insert(key, next);
    }

    fn sent_angle_jump_threshold(&self) -> f64 {
        Self::parse_optional_threshold(&self.sent_angle_jump_thresholds.red_applied)
            .unwrap_or(1.0)
            .abs()
    }

    fn apply_sent_angle_jump_threshold(&mut self) -> Result<(), String> {
        let red = Self::validate_threshold_text(&self.sent_angle_jump_thresholds.red_input)?;
        let red_value = Self::parse_optional_threshold(&red).unwrap_or(1.0).abs();
        self.sent_angle_jump_thresholds.red_applied = format!("{red_value:.3}");
        Ok(())
    }

    fn angle_delta_abs(value: f64, last_value: f64) -> f64 {
        let delta = (value - last_value).abs().rem_euclid(360.0);
        delta.min(360.0 - delta)
    }

    fn emit_sent_angle_jump_alarm(
        &mut self,
        device_id: &str,
        sensor_id: usize,
        delta_abs: f64,
        cleared: bool,
    ) {
        let red = self.sent_angle_jump_threshold();
        let angle_name = match sensor_id {
            0 => "t1",
            2 => "t2",
            4 => "s",
            _ => "unknown",
        };
        self.apply_local_alarm(AlarmEvent {
            device_id: device_id.to_string(),
            alarm_id: format!("sent_angle_jump_{angle_name}"),
            level: AlarmLevel::Critical,
            message: format!(
                "{} jump={delta_abs:.3}, red={red:.3}",
                Self::sent_angle_label(sensor_id)
            ),
            raised_at: std::time::SystemTime::now(),
            cleared,
        });
    }

    fn evaluate_sent_angle_jump(&mut self, device_id: &str, sensor_id: usize, value: f64) {
        let key = (device_id.to_string(), sensor_id);
        let previous = self
            .sent_angle_jump_states
            .get(&key)
            .copied()
            .unwrap_or_default();
        let Some(last_value) = previous.last_value else {
            self.sent_angle_jump_states.insert(
                key,
                SentAngleJumpState {
                    last_value: Some(value),
                    ..previous
                },
            );
            return;
        };

        let delta_abs = Self::angle_delta_abs(value, last_value);
        let red = self.sent_angle_jump_threshold();
        let next_active = delta_abs >= red;
        let mut next = previous;
        next.last_value = Some(value);
        next.last_delta = delta_abs;

        if next_active != previous.active {
            if previous.active {
                self.emit_sent_angle_jump_alarm(device_id, sensor_id, delta_abs, true);
            }
            if next_active {
                next.alert_count = next.alert_count.saturating_add(1);
                self.emit_sent_angle_jump_alarm(device_id, sensor_id, delta_abs, false);
            }
        }

        next.active = next_active;
        self.sent_angle_jump_states.insert(key, next);
    }

    fn apply_signal_sample(&mut self, sample: SignalSample) {
        let Some(spec) = self.signal_processor.spec(&sample.signal_id).cloned() else {
            return;
        };

        match spec.kind {
            SignalKind::SourceSensor { sensor_id } => {
                let is_tcp = sample.device_id.starts_with("tcp://");
                let msg = TelemetryMsg {
                    device_id: sample.device_id,
                    sensor_id,
                    t_sec: sample.t_sec,
                    value: sample.value,
                    request_id: sample.req_id,
                    alarm_bit: false,
                    source_kind: TelemetrySourceKind::Unknown,
                };
                if is_tcp {
                    self.tcp_sensors[sensor_id].push(&msg);
                } else {
                    self.sensors[sensor_id].push(&msg);
                    self.evaluate_can_threshold_alarms(&msg.device_id, sensor_id, msg.value);
                }
            }
            SignalKind::Derived { .. } => {
                let device_id = sample.device_id.clone();
                let signal_id = sample.signal_id;
                let msg = TelemetryMsg {
                    device_id: device_id.clone(),
                    sensor_id: 0,
                    t_sec: sample.t_sec,
                    value: sample.value,
                    request_id: sample.req_id,
                    alarm_bit: false,
                    source_kind: TelemetrySourceKind::Unknown,
                };
                self.derived_signals
                    .entry((device_id, signal_id))
                    .or_insert_with(SensorSeries::new)
                    .push(&msg);
            }
        }
    }

    fn sensor_label(&self, sensor_id: usize) -> String {
        let signal_id = format!("sensor_{sensor_id}_raw");
        if let Some(spec) = self.signal_processor.spec(&signal_id) {
            format!("{} [{}]", spec.name, spec.unit)
        } else {
            format!("Sensor {}", sensor_id)
        }
    }

    fn signal_value_text(&self, signal_id: &str, value: f64) -> String {
        if let Some(spec) = self.signal_processor.spec(signal_id) {
            format!("{:.*} {}", spec.decimals, value, spec.unit)
        } else {
            format!("{value:.3}")
        }
    }

    fn latest_demo_derived_angle_text(
        &self,
        binding: SignalBinding,
        device_id: &str,
    ) -> Option<String> {
        if device_id.is_empty() {
            return None;
        }
        let signal_id = binding.demo_derived_angle_id()?;
        let series = self
            .derived_signals
            .get(&(device_id.to_string(), signal_id.to_string()))?;
        let value = series.latest?;
        Some(self.signal_value_text(signal_id, value))
    }

    fn alarm_level_color(level: &AlarmLevel) -> egui::Color32 {
        match level {
            AlarmLevel::Info => egui::Color32::from_rgb(70, 140, 255),
            AlarmLevel::Warning => egui::Color32::from_rgb(255, 180, 0),
            AlarmLevel::Critical => egui::Color32::from_rgb(235, 64, 52),
            AlarmLevel::Purple => egui::Color32::from_rgb(165, 88, 255),
        }
    }

    fn alarm_level_rank(level: &AlarmLevel) -> u8 {
        match level {
            AlarmLevel::Info => 1,
            AlarmLevel::Warning => 2,
            AlarmLevel::Critical => 3,
            AlarmLevel::Purple => 4,
        }
    }

    fn active_alarm_summary_color(&self) -> egui::Color32 {
        match self
            .active_alarms
            .values()
            .map(|item| Self::alarm_level_rank(&item.event.level))
            .max()
        {
            Some(4) => Self::alarm_level_color(&AlarmLevel::Purple),
            Some(3) => Self::alarm_level_color(&AlarmLevel::Critical),
            Some(2) => Self::alarm_level_color(&AlarmLevel::Warning),
            Some(1) => Self::alarm_level_color(&AlarmLevel::Info),
            _ => egui::Color32::LIGHT_GREEN,
        }
    }

    fn alarm_level_text(level: &AlarmLevel) -> &'static str {
        match level {
            AlarmLevel::Info => "INFO",
            AlarmLevel::Warning => "WARNING",
            AlarmLevel::Critical => "CRITICAL",
            AlarmLevel::Purple => "PURPLE",
        }
    }

    fn demo_alarm_indicator(&self) -> Option<(&AlarmViewItem, bool)> {
        self.active_alarms
            .values()
            .find(|item| item.event.alarm_id == Self::DEMO_ALARM_ID)
            .map(|item| (item, true))
            .or_else(|| {
                self.alarm_history
                    .iter()
                    .find(|item| item.event.alarm_id == Self::DEMO_ALARM_ID)
                    .map(|item| (item, false))
            })
    }

    fn draw_demo_alarm_indicator(&self, ctx: &egui::Context) {
        if self.selected_view != TestSignalView::Demo {
            return;
        }

        egui::Window::new("demo_alarm_indicator")
            .title_bar(false)
            .resizable(false)
            .collapsible(false)
            .fixed_pos(egui::pos2(424.0, 90.0))
            .fixed_size(egui::vec2(210.0, 96.0))
            .show(ctx, |ui| {
                let (label, color, detail) = match self.demo_alarm_indicator() {
                    Some((_, true)) => (
                        "ALARM",
                        egui::Color32::from_rgb(220, 64, 52),
                        format!("bit={}", self.demo_alarm_bit_state.unwrap_or(true)),
                    ),
                    Some((_, false)) => (
                        "CLEAR",
                        egui::Color32::from_rgb(64, 170, 92),
                        format!("bit={}", self.demo_alarm_bit_state.unwrap_or(false)),
                    ),
                    None => (
                        "NORMAL",
                        egui::Color32::from_rgb(64, 170, 92),
                        format!(
                            "bit={}",
                            self.demo_alarm_bit_state.unwrap_or(false)
                        ),
                    ),
                };

                ui.vertical_centered(|ui| {
                    ui.heading("DEMO Alarm");
                    ui.add_space(6.0);
                    ui.colored_label(color, egui::RichText::new(label).size(28.0).strong());
                    ui.label(detail);
                });
            });
    }

/*    fn draw_can_alarm_indicator(&self, ctx: &egui::Context) {
        if self.selected_view != TestSignalView::CanFrame {
            return;
        }

        egui::Window::new("告警指示器")
            .title_bar(true)
            .resizable(false)
            .collapsible(false)
            .default_pos(egui::pos2(648.0, 40.0))
            .fixed_size(egui::vec2(260.0, 110.0))
            .show(ctx, |ui| {
                let (label, color, detail, message) = match self.can_alarm_indicator() {
                    Some((item, true)) => (
                        "告警",
                        Self::alarm_level_color(&item.event.level),
                        "当前激活".to_string(),
                        format!("{} | {}", item.event.alarm_id, item.event.device_id),
                    ),
                    Some((item, false)) => (
                        "已清除",
                        egui::Color32::from_rgb(64, 170, 92),
                        "最近一次".to_string(),
                        format!("{} | {}", item.event.alarm_id, item.event.device_id),
                    ),
                    None => (
                        "正常",
                        egui::Color32::from_rgb(64, 170, 92),
                        "没有can告警".to_string(),
                        "等待告警事件...".to_string(),
                    ),
                };

                ui.vertical_centered(|ui| {
                    ui.heading("CAN 告警");
                    ui.add_space(6.0);
                    ui.colored_label(color, egui::RichText::new(label).size(28.0).strong());
                    ui.label(detail);
                    ui.small(message);
                });
            });
    }

*/
    fn can_alarm_indicator(&self) -> Option<(&AlarmViewItem, bool)> {
        self.active_alarms
            .values()
            .find(|item| item.event.device_id.starts_with("can://"))
            .map(|item| (item, true))
            .or_else(|| {
                self.alarm_history
                    .iter()
                    .find(|item| item.event.device_id.starts_with("can://"))
                    .map(|item| (item, false))
            })
    }

    fn draw_can_alarm_indicator(&self, ctx: &egui::Context) {
        if self.selected_view != TestSignalView::CanFrame {
            return;
        }

        egui::Window::new("CAN Alarm")
            .title_bar(true)
            .resizable(false)
            .collapsible(false)
            .default_pos(egui::pos2(648.0, 40.0))
            .fixed_size(egui::vec2(260.0, 110.0))
            .show(ctx, |ui| {
                let (label, color, detail, message) = match self.can_alarm_indicator() {
                    Some((item, true)) => (
                        "告警",
                        Self::alarm_level_color(&item.event.level),
                        "当前激活".to_string(),
                        format!("{} | {}", item.event.alarm_id, item.event.device_id),
                    ),
                    Some((item, false)) => (
                        "已清除",
                        egui::Color32::from_rgb(64, 170, 92),
                        "最近一次".to_string(),
                        format!("{} | {}", item.event.alarm_id, item.event.device_id),
                    ),
                    None => (
                        "正常",
                        egui::Color32::from_rgb(64, 170, 92),
                        "没有 CAN 告警".to_string(),
                        "等待告警事件...".to_string(),
                    ),
                };

                ui.vertical_centered(|ui| {
                    ui.heading("CAN 告警");
                    ui.add_space(6.0);
                    ui.colored_label(color, egui::RichText::new(label).size(28.0).strong());
                    ui.label(detail);
                    ui.small(message);
                });
            });
    }

    fn sent_alarm_indicator(&self) -> Option<(&AlarmViewItem, bool)> {
        self.active_alarms
            .values()
            .find(|item| item.event.alarm_id.starts_with("sent_error_"))
            .map(|item| (item, true))
    }

    fn draw_sent_alarm_indicator(&self, ctx: &egui::Context) {
        if self.selected_view != TestSignalView::Sent {
            return;
        }

        egui::Window::new("SENT Alarm")
            .title_bar(true)
            .resizable(false)
            .collapsible(false)
            .default_pos(egui::pos2(648.0, 40.0))
            .fixed_size(egui::vec2(280.0, 110.0))
            .show(ctx, |ui| {
                let (label, color, detail, message) = match self.sent_alarm_indicator() {
                    Some((item, true)) => (
                        "ALARM",
                        Self::alarm_level_color(&item.event.level),
                        "active".to_string(),
                        format!("{} | {}", item.event.alarm_id, item.event.message),
                    ),
                    Some((item, false)) => (
                        "LAST",
                        egui::Color32::from_rgb(255, 180, 0),
                        "latest SENT alarm".to_string(),
                        format!("{} | {}", item.event.alarm_id, item.event.message),
                    ),
                    None => (
                        "OK",
                        egui::Color32::from_rgb(64, 170, 92),
                        "no SENT alarm".to_string(),
                        "waiting for SENT error frame...".to_string(),
                    ),
                };

                ui.vertical_centered(|ui| {
                    ui.heading("SENT Alarm");
                    ui.add_space(6.0);
                    ui.colored_label(color, egui::RichText::new(label).size(28.0).strong());
                    ui.label(detail);
                    ui.small(message);
                });
            });
    }

    fn sent_jump_color(level: SentJumpLevel) -> egui::Color32 {
        match level {
            SentJumpLevel::Normal => egui::Color32::from_rgb(64, 170, 92),
            SentJumpLevel::Warning => egui::Color32::from_rgb(255, 180, 0),
            SentJumpLevel::Critical => egui::Color32::from_rgb(220, 64, 52),
            SentJumpLevel::Purple => egui::Color32::from_rgb(165, 88, 255),
        }
    }

    fn sent_jump_level_text(level: SentJumpLevel) -> &'static str {
        match level {
            SentJumpLevel::Normal => "OK",
            SentJumpLevel::Warning => "WARN",
            SentJumpLevel::Critical => "RED",
            SentJumpLevel::Purple => "PURPLE",
        }
    }

    fn sent_jump_state_for_sensor(&self, sensor_id: usize) -> SentTorqueJumpState {
        self.sent_jump_states
            .iter()
            .filter(|((_, existing_sensor_id), _)| *existing_sensor_id == sensor_id)
            .map(|(_, state)| *state)
            .max_by_key(|state| match state.level {
                SentJumpLevel::Purple => 3,
                SentJumpLevel::Critical => 2,
                SentJumpLevel::Warning => 1,
                SentJumpLevel::Normal => 0,
            })
            .unwrap_or_default()
    }

    fn sent_angle_jump_state_for_sensor(&self, sensor_id: usize) -> SentAngleJumpState {
        self.sent_angle_jump_states
            .iter()
            .filter(|((_, existing_sensor_id), _)| *existing_sensor_id == sensor_id)
            .map(|(_, state)| *state)
            .max_by_key(|state| u8::from(state.active))
            .unwrap_or_default()
    }

    fn draw_sent_jump_indicator(&self, ctx: &egui::Context) {
        if self.selected_view != TestSignalView::Sent {
            return;
        }

        egui::Window::new("SENT Torque Jump")
            .title_bar(true)
            .resizable(false)
            .collapsible(false)
            .default_pos(egui::pos2(648.0, 160.0))
            .fixed_size(egui::vec2(280.0, 145.0))
            .show(ctx, |ui| {
                ui.heading("SENT torque jump");
                ui.add_space(6.0);
                for sensor_id in [1usize, 3usize] {
                    let state = self.sent_jump_state_for_sensor(sensor_id);
                    let color = Self::sent_jump_color(state.level);
                    ui.horizontal(|ui| {
                        ui.colored_label(color, egui::RichText::new("●").size(24.0));
                        ui.label(Self::sent_torque_label(sensor_id));
                        ui.label(Self::sent_jump_level_text(state.level));
                    });
                    ui.small(format!("alert_count={}", state.alert_count));
                }
            });
    }

    fn draw_sent_angle_jump_indicator(&self, ctx: &egui::Context) {
        if self.selected_view != TestSignalView::Sent {
            return;
        }

        egui::Window::new("SENT Angle Jump")
            .title_bar(true)
            .resizable(false)
            .collapsible(false)
            .default_pos(egui::pos2(648.0, 320.0))
            .fixed_size(egui::vec2(280.0, 130.0))
            .show(ctx, |ui| {
                ui.heading("SENT angle jump");
                ui.add_space(6.0);
                for sensor_id in [0usize, 2usize, 4usize] {
                    let state = self.sent_angle_jump_state_for_sensor(sensor_id);
                    let color = if state.active {
                        egui::Color32::from_rgb(220, 64, 52)
                    } else {
                        egui::Color32::from_rgb(64, 170, 92)
                    };
                    ui.horizontal(|ui| {
                        ui.colored_label(color, egui::RichText::new("●").size(24.0));
                        ui.label(Self::sent_angle_label(sensor_id));
                        ui.label(if state.active { "RED" } else { "OK" });
                    });
                    ui.small(format!("alert_count={}", state.alert_count));
                }
            });
    }

    fn draw_sent_jump_threshold_panel(&mut self, ctx: &egui::Context) {
        if self.selected_view != TestSignalView::Sent {
            return;
        }

        egui::Window::new("SENT Thresholds")
            .title_bar(true)
            .resizable(false)
            .collapsible(false)
            .default_pos(egui::pos2(940.0, 160.0))
            .fixed_size(egui::vec2(300.0, 178.0))
            .show(ctx, |ui| {
                ui.heading("SENT torque jump thresholds");
                ui.add_space(6.0);
                egui::Grid::new("sent_jump_threshold_grid")
                    .num_columns(2)
                    .spacing(egui::vec2(8.0, 8.0))
                    .show(ui, |ui| {
                        ui.label("yellow >");
                        ui.add(
                            egui::TextEdit::singleline(
                                &mut self.sent_jump_thresholds.warn_input,
                            )
                            .desired_width(96.0),
                        );
                        ui.end_row();

                        ui.label("red >=");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.sent_jump_thresholds.red_input)
                                .desired_width(96.0),
                        );
                        ui.end_row();

                        ui.label("purple >=");
                        ui.add(
                            egui::TextEdit::singleline(
                                &mut self.sent_jump_thresholds.purple_input,
                            )
                            .desired_width(96.0),
                        );
                        ui.end_row();
                    });

                ui.add_space(8.0);
                if ui.button("Apply").clicked() {
                    self.status = match self.apply_sent_jump_thresholds() {
                        Ok(()) => "SENT torque jump thresholds applied".to_string(),
                        Err(err) => format!("SENT torque jump threshold error: {err}"),
                    };
                }
                let (warn, red, purple) = self.sent_jump_thresholds();
                ui.small(format!(
                    "current: green <= {warn:.3}, yellow > {warn:.3}, red >= {red:.3}, purple >= {purple:.3}"
                ));
            });
    }

    fn draw_sent_angle_jump_threshold_panel(&mut self, ctx: &egui::Context) {
        if self.selected_view != TestSignalView::Sent {
            return;
        }

        egui::Window::new("SENT Angle Threshold")
            .title_bar(true)
            .resizable(false)
            .collapsible(false)
            .default_pos(egui::pos2(940.0, 352.0))
            .fixed_size(egui::vec2(300.0, 118.0))
            .show(ctx, |ui| {
                ui.heading("SENT angle jump threshold");
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    ui.label("red >=");
                    ui.add(
                        egui::TextEdit::singleline(
                            &mut self.sent_angle_jump_thresholds.red_input,
                        )
                        .desired_width(96.0),
                    );
                });

                ui.add_space(8.0);
                if ui.button("Apply").clicked() {
                    self.status = match self.apply_sent_angle_jump_threshold() {
                        Ok(()) => "SENT angle jump threshold applied".to_string(),
                        Err(err) => format!("SENT angle jump threshold error: {err}"),
                    };
                }
                let red = self.sent_angle_jump_threshold();
                ui.small(format!("current: green < {red:.3}, red >= {red:.3}"));
            });
    }

    fn draw_can_threshold_panel(&mut self, ctx: &egui::Context) {
        if self.selected_view != TestSignalView::CanFrame {
            return;
        }

        egui::Window::new("阈值面板")
            .title_bar(true)
            .resizable(false)
            .collapsible(false)
            .default_pos(egui::pos2(924.0, 0.0))
            .fixed_size(egui::vec2(400.0, 190.0))
            .show(ctx, |ui| {
                ui.heading("告警阈值");

                ui.add_space(6.0);

                egui::Grid::new("can_alarm_threshold_grid")
                    .num_columns(3)
                    .spacing(egui::vec2(8.0, 8.0))
                    .show(ui, |ui| {
                        ui.label("轴");
                        ui.label("上限");
                        ui.label("下限");
                        ui.end_row();

                        ui.label("X");
                        ui.add(
                            egui::TextEdit::singleline(
                                &mut self.can_alarm_thresholds.x_high_input,
                            )
                            .desired_width(88.0),
                        );
                        ui.add(
                            egui::TextEdit::singleline(
                                &mut self.can_alarm_thresholds.x_low_input,
                            )
                            .desired_width(40.0),
                        );
                        ui.end_row();

                        ui.label("Y");
                        ui.add(
                            egui::TextEdit::singleline(
                                &mut self.can_alarm_thresholds.y_high_input,
                            )
                            .desired_width(88.0),
                        );
                        ui.add(
                            egui::TextEdit::singleline(
                                &mut self.can_alarm_thresholds.y_low_input,
                            )
                            .desired_width(40.0),
                        );
                        ui.end_row();

                        ui.label("Z");
                        ui.add(
                            egui::TextEdit::singleline(
                                &mut self.can_alarm_thresholds.z_high_input,
                            )
                            .desired_width(88.0),
                        );
                        ui.add(
                            egui::TextEdit::singleline(
                                &mut self.can_alarm_thresholds.z_low_input,
                            )
                            .desired_width(40.0),
                        );
                        ui.end_row();
                    });

                ui.add_space(8.0);
                if ui.button("确认阈值").clicked() {
                    self.status = match self.apply_can_alarm_thresholds() {
                        Ok(()) => "CAN 告警阈值已确认生效".to_string(),
                        Err(err) => format!("CAN 告警阈值确认失败: {err}"),
                    };
                }
            });
    }

    fn clear_alarm_panel_stats(&mut self) {
        self.active_alarms.clear();
        self.alarm_history.clear();
        self.total_alarm_count = 0;
        self.can_alarm_states.clear();
        self.sent_jump_states.clear();
        self.sent_angle_jump_states.clear();
        self.status = "报警面板和报警统计已清除".to_string();
    }

    fn draw_alarm_panel(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("alarm_panel")
            .resizable(true)
            .default_height(300.0)
            .min_height(520.0)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label(format!("报警次数: {}", self.total_alarm_count));
                    ui.separator();
                    if ui.button("清除报警").clicked() {
                        self.clear_alarm_panel_stats();
                    }
                    ui.separator();
                    ui.heading("报警");
                    ui.separator();
                    ui.colored_label(
                        self.active_alarm_summary_color(),
                        format!("当前活跃: {}", self.active_alarms.len()),
                    );
                    ui.label(format!("最近记录: {}", self.alarm_history.len()));
                });

                ui.add_space(6.0);
                ui.columns(2, |columns| {
                    columns[0].group(|ui| {
                        ui.label("当前报警");
                        ui.separator();
                        if self.active_alarms.is_empty() {
                            ui.label("暂无活跃报警");
                        } else {
                            egui::ScrollArea::vertical()
                                .id_salt("active_alarm_scroll")
                                .show(ui, |ui| {
                                let mut items: Vec<_> = self.active_alarms.values().collect();
                                items.sort_by_key(|item| std::cmp::Reverse(item.received_at));
                                for item in items {
                                    let level_color = Self::alarm_level_color(&item.event.level);
                                    ui.horizontal_wrapped(|ui| {
                                        ui.colored_label(
                                            level_color,
                                            Self::alarm_level_text(&item.event.level),
                                        );
                                        ui.label(format_alarm_datetime(item.event.raised_at));
                                        ui.label(&item.event.device_id);
                                        ui.label(
                                            egui::RichText::new(item.event.alarm_id.as_str())
                                                .monospace()
                                                .color(level_color),
                                        );
                                    });
                                    ui.colored_label(level_color, &item.event.message);
                                    ui.separator();
                                }
                                });
                        }
                    });

                    columns[1].group(|ui| {
                        ui.label("最近报警记录");
                        ui.separator();
                        if self.alarm_history.is_empty() {
                            ui.label("还没有收到报警事件");
                        } else {
                            egui::ScrollArea::vertical()
                                .id_salt("alarm_history_scroll")
                                .show(ui, |ui| {
                                for item in &self.alarm_history {
                                    let level_color = Self::alarm_level_color(&item.event.level);
                                    ui.horizontal_wrapped(|ui| {
                                        ui.colored_label(
                                            level_color,
                                            Self::alarm_level_text(&item.event.level),
                                        );
                                        ui.label(format_alarm_datetime(item.event.raised_at));
                                        ui.label(if item.event.cleared {
                                            "已恢复"
                                        } else {
                                            "已触发"
                                        });
                                        ui.label(&item.event.device_id);
                                    });
                                    ui.label(
                                        egui::RichText::new(item.event.alarm_id.as_str())
                                            .monospace()
                                            .color(level_color),
                                    );
                                    ui.colored_label(level_color, &item.event.message);
                                    ui.separator();
                                }
                                });
                        }
                    });
                });
            });
    }

    fn push_dynamic_window(&mut self, title: String, binding: Option<SignalBinding>) {
        let position = egui::pos2(
            120.0 + self.dynamic_windows.len() as f32 * 28.0,
            120.0 + self.dynamic_windows.len() as f32 * 24.0,
        );
        self.dynamic_windows.push(DynamicSignalWindow {
            title,
            binding,
            position,
            scale: 1.0,
            rect: None,
        });
    }

    fn add_dynamic_window(&mut self) {
        match self.selected_view {
            TestSignalView::Demo => {
                let index = self.demo_group_index;
                self.push_dynamic_window(
                    SignalBinding::DemoAxisX.title(index),
                    Some(SignalBinding::DemoAxisX),
                );
                self.push_dynamic_window(
                    SignalBinding::DemoAxisY.title(index),
                    Some(SignalBinding::DemoAxisY),
                );
                self.push_dynamic_window(
                    SignalBinding::DemoAxisZ.title(index),
                    Some(SignalBinding::DemoAxisZ),
                );
                self.demo_group_index = self.demo_group_index.saturating_add(1);
            }
            TestSignalView::Sent => {
                let index = self.sent1_group_index;
                self.push_dynamic_window(
                    SignalBinding::Sent1V1.title(index),
                    Some(SignalBinding::Sent1V1),
                );
                self.push_dynamic_window(
                    SignalBinding::Sent1P1.title(index),
                    Some(SignalBinding::Sent1P1),
                );
                self.push_dynamic_window(
                    SignalBinding::Sent2V2.title(index),
                    Some(SignalBinding::Sent2V2),
                );
                self.push_dynamic_window(
                    SignalBinding::Sent2P2.title(index),
                    Some(SignalBinding::Sent2P2),
                );
                self.push_dynamic_window(
                    SignalBinding::Sent3Angle.title(index),
                    Some(SignalBinding::Sent3Angle),
                );
                self.sent1_group_index = self.sent1_group_index.saturating_add(1);
            }
            TestSignalView::CanFrame => {
                let index = self.can_group_index;
                self.push_dynamic_window(
                    SignalBinding::CanAxisX.title(index),
                    Some(SignalBinding::CanAxisX),
                );
                self.push_dynamic_window(
                    SignalBinding::CanAxisY.title(index),
                    Some(SignalBinding::CanAxisY),
                );
                self.push_dynamic_window(
                    SignalBinding::CanAxisZ.title(index),
                    Some(SignalBinding::CanAxisZ),
                );
                self.can_group_index = self.can_group_index.saturating_add(1);
            }
            TestSignalView::TcpFrame => {
                let index = self.tcp_group_index;
                let start_sensor = self
                    .dynamic_windows
                    .iter()
                    .filter_map(|window| match window.binding {
                        Some(SignalBinding::TcpSensor(sensor_id)) => Some(sensor_id),
                        _ => None,
                    })
                    .max()
                    .unwrap_or(0);
                for sensor_id in start_sensor..start_sensor.saturating_add(4) {
                    let binding = SignalBinding::TcpSensor(sensor_id);
                    self.push_dynamic_window(binding.title(index), Some(binding));
                }
                self.tcp_group_index = self.tcp_group_index.saturating_add(1);
            }
        }
    }

    fn apply_ctrl_wheel_zoom(&mut self, ctx: &egui::Context) {
        let (ctrl, scroll_y, pointer_pos) = ctx.input(|i| {
            (
                i.modifiers.ctrl,
                i.raw_scroll_delta.y,
                i.pointer.hover_pos(),
            )
        });

        if !ctrl || scroll_y.abs() < f32::EPSILON {
            return;
        }
        let Some(pointer) = pointer_pos else {
            return;
        };

        for window in &mut self.dynamic_windows {
            if let Some(rect) = window.rect {
                if rect.contains(pointer) {
                    let factor = (1.0 + scroll_y * 0.0015).clamp(0.85, 1.2);
                    window.scale = (window.scale * factor).clamp(SCALE_MIN, SCALE_MAX);
                    break;
                }
            }
        }

    }

    fn draw_sensor_chart(
        ui: &mut egui::Ui,
        points: &VecDeque<[f64; 2]>,
        height: f32,
        label: &str,
        thresholds: Option<(Option<f64>, Option<f64>)>,
    ) {
        let desired_size = egui::vec2(ui.available_width(), height.max(40.0));
        let (rect, _) = ui.allocate_exact_size(desired_size, egui::Sense::hover());
        let painter = ui.painter_at(rect);

        painter.rect_stroke(
            rect,
            4.0,
            egui::Stroke::new(1.0, egui::Color32::DARK_GRAY),
            egui::StrokeKind::Outside,
        );

        let (high_threshold, low_threshold) = thresholds.unwrap_or((None, None));
        if points.len() < 2 && high_threshold.is_none() && low_threshold.is_none() {
            painter.text(
                rect.center(),
                egui::Align2::CENTER_CENTER,
                label,
                egui::FontId::proportional(12.0),
                egui::Color32::GRAY,
            );
            return;
        }

        let min_x = points.front().map(|p| p[0]).unwrap_or(0.0);
        let max_x = points.back().map(|p| p[0]).unwrap_or(min_x + 1.0);
        let mut min_y = f64::INFINITY;
        let mut max_y = f64::NEG_INFINITY;
        for p in points {
            min_y = min_y.min(p[1]);
            max_y = max_y.max(p[1]);
        }
        if !min_y.is_finite() || !max_y.is_finite() {
            min_y = -1.0;
            max_y = 1.0;
        }
        if (max_y - min_y).abs() < f64::EPSILON {
            max_y += 1.0;
            min_y -= 1.0;
        }
        let pad = (max_y - min_y) * Y_RANGE_PADDING_RATIO;
        max_y += pad;
        min_y -= pad;

        let to_screen = |x: f64, y: f64| -> egui::Pos2 {
            let tx = ((x - min_x) / (max_x - min_x + f64::EPSILON)) as f32;
            let ty = ((y - min_y) / (max_y - min_y + f64::EPSILON)) as f32;
            egui::pos2(
                rect.left() + tx * rect.width(),
                rect.bottom() - ty * rect.height(),
            )
        };

        let draw_threshold_line = |value: f64, label: &str| {
            let y = to_screen(min_x, value)
                .y
                .clamp(rect.top() + 2.0, rect.bottom() - 2.0);
            let color = egui::Color32::from_rgb(220, 64, 52);
            painter.line_segment(
                [egui::pos2(rect.left(), y), egui::pos2(rect.right(), y)],
                egui::Stroke::new(1.1, color),
            );
            painter.text(
                egui::pos2(rect.right() - 4.0, y - 2.0),
                egui::Align2::RIGHT_BOTTOM,
                label,
                egui::FontId::proportional(10.0),
                color,
            );
        };

        if let Some(value) = high_threshold {
            draw_threshold_line(value, &format!("H {:.2}", value));
        }
        if let Some(value) = low_threshold {
            draw_threshold_line(value, &format!("L {:.2}", value));
        }

        let display_max_y = match high_threshold {
            Some(value) if value > max_y => value,
            _ => max_y,
        };
        let display_min_y = match low_threshold {
            Some(value) if value < min_y => value,
            _ => min_y,
        };

        let mut segment: Vec<egui::Pos2> = Vec::with_capacity(points.len());
        let mut prev_t: Option<f64> = None;
        for p in points {
            if let Some(pt) = prev_t {
                if p[0] - pt > LINE_BREAK_GAP_SECS {
                    if segment.len() >= 2 {
                        painter.add(egui::Shape::line(
                            std::mem::take(&mut segment),
                            egui::Stroke::new(1.6, egui::Color32::LIGHT_GREEN),
                        ));
                    } else {
                        segment.clear();
                    }
                }
            }
            segment.push(to_screen(p[0], p[1]));
            prev_t = Some(p[0]);
        }
        if segment.len() >= 2 {
            painter.add(egui::Shape::line(
                segment,
                egui::Stroke::new(1.6, egui::Color32::LIGHT_GREEN),
            ));
        }

        // Show dynamic y-bounds for this rolling chart window.
        painter.text(
            rect.left_top() + egui::vec2(6.0, 4.0),
            egui::Align2::LEFT_TOP,
            format!("上界 {:.3}", display_max_y),
            egui::FontId::proportional(11.0),
            egui::Color32::LIGHT_BLUE,
        );
        painter.text(
            rect.left_bottom() + egui::vec2(6.0, -4.0),
            egui::Align2::LEFT_BOTTOM,
            format!("下界 {:.3}", display_min_y),
            egui::FontId::proportional(11.0),
            egui::Color32::LIGHT_BLUE,
        );

        // Mark latest point and keep its value label moving with the point.
        if let Some(last) = points.back() {
            let p = to_screen(last[0], last[1]);
            painter.circle_filled(p, 3.5, egui::Color32::YELLOW);

            let label_pos = egui::pos2(
                (p.x + 8.0).clamp(rect.left() + 6.0, rect.right() - 70.0),
                (p.y - 8.0).clamp(rect.top() + 16.0, rect.bottom() - 6.0),
            );
            painter.line_segment([p, label_pos], egui::Stroke::new(1.0, egui::Color32::GOLD));
            painter.text(
                label_pos,
                egui::Align2::LEFT_BOTTOM,
                format!("{:.3}", last[1]),
                egui::FontId::proportional(11.0),
                egui::Color32::YELLOW,
            );
        }
    }

    fn open_can_replay(&mut self) {
        self.can_replay.open = true;
        if self.can_replay.start_ts_input.trim().is_empty()
            || self.can_replay.end_ts_input.trim().is_empty()
        {
            let end_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let start_ms = end_ms.saturating_sub(CAN_REPLAY_DEFAULT_WINDOW_MS);
            self.can_replay.start_ts_input = format_datetime_input(start_ms);
            self.can_replay.end_ts_input = format_datetime_input(end_ms);
        }
    }

    fn request_can_replay_load(&mut self) {
        if self.can_replay.loading {
            self.can_replay.status = "正在加载，请稍候".to_string();
            return;
        }

        let start_ts_ms = match parse_time_input(&self.can_replay.start_ts_input) {
            Some(v) => v,
            None => {
                self.can_replay.status =
                    "开始时间请输入 ts_ms 或 YYYY-MM-DD HH:MM:SS".to_string();
                return;
            }
        };
        let end_ts_ms = match parse_time_input(&self.can_replay.end_ts_input) {
            Some(v) => v,
            None => {
                self.can_replay.status =
                    "结束时间请输入 ts_ms 或 YYYY-MM-DD HH:MM:SS".to_string();
                return;
            }
        };
        if end_ts_ms <= start_ts_ms {
            self.can_replay.status = "结束时间必须大于开始时间".to_string();
            return;
        }

        self.can_replay.loading = true;
        let dsn = self.can_replay.pg_dsn.clone();
        let mode = self.can_replay.mode;
        self.can_replay.status = self.can_replay.mode.load_status().to_string();
        let tx = self.ui_tx.clone();
        thread::spawn(move || {
            let result = (|| -> Result<CanReplayData, String> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| err.to_string())?;
                rt.block_on(async move {
                    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
                        .await
                        .map_err(|err| format!("connect postgres failed: {err}; dsn={dsn}"))?;
                    tokio::spawn(async move {
                        let _ = connection.await;
                    });

                    let axes = mode.axis_filters();
                    let rows = client
                        .query(
                            "SELECT ts_ms, axis, value
                             FROM telemetry_samples
                             WHERE device_id LIKE 'can://%'
                               AND ts_ms >= $1
                               AND ts_ms <= $2
                               AND axis IN ($3, $4, $5, $6, $7)
                             ORDER BY ts_ms ASC, axis ASC",
                            &[
                                &start_ts_ms,
                                &end_ts_ms,
                                &axes[0],
                                &axes[1],
                                &axes[2],
                                &axes[3],
                                &axes[4],
                            ],
                        )
                        .await
                        .map_err(|err| err.to_string())?;

                    let mut data = CanReplayData {
                        min_ts_ms: start_ts_ms,
                        max_ts_ms: end_ts_ms,
                        ..CanReplayData::default()
                    };
                    for row in rows {
                        let ts_ms: i64 = row.get(0);
                        let axis: String = row.get(1);
                        let value: f64 = row.get(2);
                        let x_sec = (ts_ms - start_ts_ms) as f64 / 1000.0;
                        match axis.as_str() {
                            axis_name if axis_name == axes[0] => data.x_points.push([x_sec, value]),
                            axis_name if axis_name == axes[1] => data.y_points.push([x_sec, value]),
                            axis_name if axis_name == axes[2] => data.z_points.push([x_sec, value]),
                            axis_name if axis_name == axes[3] => data.u_points.push([x_sec, value]),
                            axis_name if axis_name == axes[4] => data.v_points.push([x_sec, value]),
                            _ => {}
                        }
                    }

                    let alarm_rows = client
                        .query(
                            "SELECT ts_ms, alarm_id, level, message, cleared
                             FROM alarm_events
                             WHERE ts_ms >= $1
                               AND ts_ms <= $2
                               AND (alarm_id LIKE 'can_%' OR alarm_id LIKE 'sent_%')
                             ORDER BY ts_ms ASC, id ASC",
                            &[&start_ts_ms, &end_ts_ms],
                        )
                        .await
                        .map_err(|err| err.to_string())?;

                    for row in alarm_rows {
                        let ts_ms: i64 = row.get(0);
                        let alarm_id: String = row.get(1);
                        let level: String = row.get(2);
                        let message: String = row.get(3);
                        let cleared: bool = row.get(4);
                        let x_sec = (ts_ms - start_ts_ms) as f64 / 1000.0;
                        match mode {
                            ReplayMode::Can3Axis => {
                                let target = if alarm_id.starts_with("can_x_") {
                                    Some((&data.x_points, &mut data.x_alarm_points))
                                } else if alarm_id.starts_with("can_y_") {
                                    Some((&data.y_points, &mut data.y_alarm_points))
                                } else if alarm_id.starts_with("can_z_") {
                                    Some((&data.z_points, &mut data.z_alarm_points))
                                } else {
                                    None
                                };
                                if let Some((series, alarms)) = target {
                                    if let Some(point) = nearest_plot_point(series, x_sec) {
                                        alarms.push(AlarmPlotPoint {
                                            point: [x_sec, point[1]],
                                            level,
                                            cleared,
                                        });
                                    }
                                }
                            }
                            ReplayMode::Sent => {
                                let target = if alarm_id == "sent_torque_jump_t1" {
                                    Some((&data.y_points, &mut data.y_alarm_points))
                                } else if alarm_id == "sent_angle_jump_t1" || message.contains("t1=1") {
                                    Some((&data.x_points, &mut data.x_alarm_points))
                                } else if alarm_id == "sent_torque_jump_t2" {
                                    Some((&data.u_points, &mut data.u_alarm_points))
                                } else if alarm_id == "sent_angle_jump_t2" || message.contains("t2=1") {
                                    Some((&data.z_points, &mut data.z_alarm_points))
                                } else if alarm_id == "sent_angle_jump_s" || message.contains("s=1") {
                                    Some((&data.v_points, &mut data.v_alarm_points))
                                } else {
                                    None
                                };
                                if let Some((series, alarms)) = target {
                                    if let Some(point) = nearest_plot_point(series, x_sec) {
                                        alarms.push(AlarmPlotPoint {
                                            point: [x_sec, point[1]],
                                            level,
                                            cleared,
                                        });
                                    }
                                }
                            }
                        }
                    }

                    Ok(data)
                })
            })();

            let _ = tx.send(UiMsg::CanReplayLoaded(mode, result));
        });
    }

    fn request_can_replay_export(&mut self) {
        if self.can_replay.exporting {
            self.can_replay.status = "正在导出，请稍候...".to_string();
            return;
        }

        let start_ts_ms = match parse_time_input(&self.can_replay.start_ts_input) {
            Some(v) => v,
            None => {
                self.can_replay.status =
                    "开始时间请输入 ts_ms 或 YYYY-MM-DD HH:MM:SS".to_string();
                return;
            }
        };
        let end_ts_ms = match parse_time_input(&self.can_replay.end_ts_input) {
            Some(v) => v,
            None => {
                self.can_replay.status =
                    "结束时间请输入 ts_ms 或 YYYY-MM-DD HH:MM:SS".to_string();
                return;
            }
        };
        if end_ts_ms <= start_ts_ms {
            self.can_replay.status = "结束时间必须大于开始时间".to_string();
            return;
        }
        let has_selected_series = self.can_replay.show_x
            || self.can_replay.show_y
            || self.can_replay.show_z
            || (self.can_replay.mode == ReplayMode::Sent
                && (self.can_replay.show_u || self.can_replay.show_v));
        if !has_selected_series {
            self.can_replay.status = "至少勾选一个轴后再导出".to_string();
            return;
        }

        self.can_replay.exporting = true;
        self.can_replay.status = "正在流式导出 txt...".to_string();
        let dsn = self.can_replay.pg_dsn.clone();
        let mode = self.can_replay.mode;
        let tx = self.ui_tx.clone();
        let show_x = self.can_replay.show_x;
        let show_y = self.can_replay.show_y;
        let show_z = self.can_replay.show_z;
        let show_u = mode == ReplayMode::Sent && self.can_replay.show_u;
        let show_v = mode == ReplayMode::Sent && self.can_replay.show_v;
        thread::spawn(move || {
            let result = (|| -> Result<String, String> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| err.to_string())?;
                rt.block_on(async move {
                    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
                        .await
                        .map_err(|err| err.to_string())?;
                    tokio::spawn(async move {
                        let _ = connection.await;
                    });

                    let now_ts_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    fs::create_dir_all(CAN_EXPORT_DIR)
                        .map_err(|err| format!("create export dir failed: {err}; dir={CAN_EXPORT_DIR}"))?;
                    let export_path = std::path::PathBuf::from(CAN_EXPORT_DIR).join(
                        default_can_export_filename(start_ts_ms, end_ts_ms, now_ts_ms),
                    );
                    let file = fs::File::create(&export_path).map_err(|err| {
                        format!(
                            "create export file failed: {err}; path={}",
                            export_path.display()
                        )
                    })?;
                    let mut writer = std::io::BufWriter::new(file);

                    use std::io::Write;
                    writeln!(writer, "{}", mode.export_header())
                        .map_err(|err| format!("write export header failed: {err}"))?;
                    writeln!(writer, "# start_ts_ms={start_ts_ms}")
                        .map_err(|err| format!("write start_ts_ms failed: {err}"))?;
                    writeln!(writer, "# end_ts_ms={end_ts_ms}")
                        .map_err(|err| format!("write end_ts_ms failed: {err}"))?;
                    writeln!(writer, "ts_ms\tdevice_id\taxis\tvalue\trequest_id")
                        .map_err(|err| format!("write export columns failed: {err}"))?;

                    let axes = mode.axis_filters();
                    let sql = "SELECT ts_ms, device_id, axis, value, request_id
                               FROM telemetry_samples
                               WHERE device_id LIKE 'can://%'
                                 AND ts_ms >= $1
                                 AND ts_ms <= $2
                                 AND (($3 AND axis = $8)
                                   OR ($4 AND axis = $9)
                                   OR ($5 AND axis = $10)
                                   OR ($6 AND axis = $11)
                                   OR ($7 AND axis = $12))
                               ORDER BY ts_ms ASC, axis ASC";

                    let params: [&(dyn tokio_postgres::types::ToSql + Sync); 12] = [
                        &start_ts_ms,
                        &end_ts_ms,
                        &show_x,
                        &show_y,
                        &show_z,
                        &show_u,
                        &show_v,
                        &axes[0],
                        &axes[1],
                        &axes[2],
                        &axes[3],
                        &axes[4],
                    ];
                    let rows = client
                        .query_raw(sql, params)
                        .await
                        .map_err(|err| format!("query export rows failed: {err}"))?;
                    pin_mut!(rows);

                    while let Some(row) = rows
                        .try_next()
                        .await
                        .map_err(|err| format!("stream export rows failed: {err}"))?
                    {
                        let ts_ms: i64 = row.get(0);
                        let device_id: String = row.get(1);
                        let axis: String = row.get(2);
                        let value: f64 = row.get(3);
                        let request_id: i64 = row.get(4);
                        writeln!(
                            writer,
                            "{ts_ms}\t{device_id}\t{axis}\t{value:.6}\t{request_id}"
                        )
                        .map_err(|err| format!("write export row failed: {err}"))?;
                    }
                    writer
                        .flush()
                        .map_err(|err| format!("flush export file failed: {err}"))?;

                    Ok(export_path.display().to_string())
                })
            })();

            let _ = tx.send(UiMsg::CanReplayExported(result));
        });
    }

    fn format_ts_ms(ts_ms: i64) -> String {
        let total_seconds = ts_ms.div_euclid(1000) + DISPLAY_TZ_OFFSET_SECS;
        let seconds_of_day = total_seconds.rem_euclid(86_400);
        let hour = seconds_of_day / 3600;
        let minute = (seconds_of_day % 3600) / 60;
        let second = seconds_of_day % 60;
        format!("{hour:02}:{minute:02}:{second:02}")
    }

    fn draw_alarm_plot_points(
        plot_ui: &mut egui_plot::PlotUi<'_>,
        label: &str,
        points: &[AlarmPlotPoint],
    ) {
        for (idx, alarm) in points.iter().enumerate() {
            let color = Self::alarm_record_level_color(&alarm.level);
            let radius = if alarm.cleared { 4.0 } else { 6.0 };
            plot_ui.points(
                Points::new(
                    format!("{label} alarm {idx}"),
                    PlotPoints::new(vec![alarm.point]),
                )
                .color(color)
                .radius(radius)
                .filled(!alarm.cleared),
            );
        }
    }

    fn draw_can_replay_chart(
        &mut self,
        ui: &mut egui::Ui,
        data: &CanReplayData,
        height: f32,
    ) -> Option<(i64, i64)> {
        let plot_response = Plot::new("can_replay_plot")
            .allow_zoom([true, true])
            .allow_scroll([true, true])
            .allow_drag([true, true])
            .height(height.max(260.0))
            .x_axis_formatter({
                let base_ts_ms = data.min_ts_ms;
                move |mark, _range| {
                    let ts_ms = base_ts_ms + (mark.value * 1000.0) as i64;
                    Self::format_ts_ms(ts_ms)
                }
            })
            .show(ui, |plot_ui| {
                let labels = self.can_replay.mode.series_labels();
                if self.can_replay.show_x {
                    plot_ui.line(
                        Line::new(labels[0], PlotPoints::new(data.x_points.clone()))
                            .color(egui::Color32::from_rgb(239, 83, 80)),
                    );
                    if self.can_replay.show_alarm_points {
                        Self::draw_alarm_plot_points(plot_ui, labels[0], &data.x_alarm_points);
                    }
                }
                if self.can_replay.show_y {
                    plot_ui.line(
                        Line::new(labels[1], PlotPoints::new(data.y_points.clone()))
                            .color(egui::Color32::from_rgb(66, 165, 245)),
                    );
                    if self.can_replay.show_alarm_points {
                        Self::draw_alarm_plot_points(plot_ui, labels[1], &data.y_alarm_points);
                    }
                }
                if self.can_replay.show_z {
                    plot_ui.line(
                        Line::new(labels[2], PlotPoints::new(data.z_points.clone()))
                            .color(egui::Color32::from_rgb(102, 187, 106)),
                    );
                    if self.can_replay.show_alarm_points {
                        Self::draw_alarm_plot_points(plot_ui, labels[2], &data.z_alarm_points);
                    }
                }
                if self.can_replay.mode == ReplayMode::Sent && self.can_replay.show_u {
                    plot_ui.line(
                        Line::new(labels[3], PlotPoints::new(data.u_points.clone()))
                            .color(egui::Color32::from_rgb(255, 167, 38)),
                    );
                    if self.can_replay.show_alarm_points {
                        Self::draw_alarm_plot_points(plot_ui, labels[3], &data.u_alarm_points);
                    }
                }
                if self.can_replay.mode == ReplayMode::Sent && self.can_replay.show_v {
                    plot_ui.line(
                        Line::new(labels[4], PlotPoints::new(data.v_points.clone()))
                            .color(egui::Color32::from_rgb(171, 71, 188)),
                    );
                    if self.can_replay.show_alarm_points {
                        Self::draw_alarm_plot_points(plot_ui, labels[4], &data.v_alarm_points);
                    }
                }
                plot_ui.plot_bounds()
            });

        self.can_replay.plot_rect = Some(plot_response.response.rect);
        let bounds = plot_response.inner;
        let x_range = bounds.range_x();
        let start_ms = data.min_ts_ms + (*x_range.start() * 1000.0) as i64;
        let end_ms = data.min_ts_ms + (*x_range.end() * 1000.0) as i64;
        Some((start_ms, end_ms))
    }

    fn draw_can_replay_window(&mut self, ctx: &egui::Context) {
        if !self.can_replay.open {
            return;
        }

        let mut open = self.can_replay.open;
        egui::Window::new("CAN 回放")
            .open(&mut open)
            .default_size(egui::vec2(1080.0, 640.0))
            .min_width(860.0)
            .min_height(520.0)
            .show(ctx, |ui| {
                ui.horizontal_wrapped(|ui| {
                    ui.label("模式");
                    let previous_mode = self.can_replay.mode;
                    ui.add_enabled_ui(!self.can_replay.loading, |ui| {
                        ui.selectable_value(&mut self.can_replay.mode, ReplayMode::Can3Axis, "3轴");
                        ui.selectable_value(&mut self.can_replay.mode, ReplayMode::Sent, "SENT");
                    });
                    if self.can_replay.mode != previous_mode {
                        self.can_replay.data = None;
                        self.can_replay.plot_rect = None;
                        self.can_replay.status = self.can_replay.mode.empty_status().to_string();
                    }
                    ui.label("开始时间");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.can_replay.start_ts_input)
                            .desired_width(220.0),
                    );
                    ui.label("结束时间");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.can_replay.end_ts_input)
                            .desired_width(220.0),
                    );
                    if ui
                        .add_enabled(!self.can_replay.loading, egui::Button::new("加载回放"))
                        .clicked()
                    {
                        self.request_can_replay_load();
                    }
                    if ui
                        .add_enabled(
                            !self.can_replay.loading && !self.can_replay.exporting,
                            egui::Button::new("导出TXT"),
                        )
                        .clicked()
                    {
                        self.request_can_replay_export();
                    }
                    if ui.button("最近5分钟").clicked() {
                        let end_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);
                        self.can_replay.start_ts_input =
                            format_datetime_input(end_ms.saturating_sub(CAN_REPLAY_DEFAULT_WINDOW_MS));
                        self.can_replay.end_ts_input = format_datetime_input(end_ms);
                    }
                });

                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    let labels = self.can_replay.mode.series_labels();
                    ui.label("轴显示");
                    ui.toggle_value(&mut self.can_replay.show_x, labels[0]);
                    ui.toggle_value(&mut self.can_replay.show_y, labels[1]);
                    ui.toggle_value(&mut self.can_replay.show_z, labels[2]);
                    if self.can_replay.mode == ReplayMode::Sent {
                        ui.toggle_value(&mut self.can_replay.show_u, labels[3]);
                        ui.toggle_value(&mut self.can_replay.show_v, labels[4]);
                    }
                    ui.separator();
                    ui.toggle_value(&mut self.can_replay.show_alarm_points, "报警点");
                });

                ui.label(&self.can_replay.status);
                ui.small("支持 ts_ms，或 YYYY-MM-DD HH:MM:SS");
                ui.add_space(6.0);

                let available_height = (ui.available_height() - 56.0).max(240.0);
                if let Some(data) = self.can_replay.data.clone() {
                    let visible_range = self.draw_can_replay_chart(ui, &data, available_height);
                    ui.horizontal(|ui| {
                        ui.label(format!("ts_ms: {}", data.min_ts_ms));
                        ui.add_space((ui.available_width() - 160.0).max(0.0));
                        ui.label(format!("ts_ms: {}", data.max_ts_ms));
                    });
                    if let Some((visible_start_ms, visible_end_ms)) = visible_range {
                        ui.label(format!(
                            "当前视图: {} -> {}",
                            visible_start_ms, visible_end_ms
                        ));
                    }
                } else {
                    self.can_replay.plot_rect = None;
                    ui.group(|ui| {
                        ui.set_min_height(available_height);
                        ui.vertical_centered(|ui| {
                            ui.add_space(available_height * 0.35);
                            ui.heading("CAN 回放");
                            ui.label("输入开始/结束时间后点击“加载回放”");
                        });
                    });
                }
            });
        self.can_replay.open = open;
    }

    fn open_alarm_records(&mut self) {
        self.alarm_records.open = true;
        if self.alarm_records.start_ts_input.trim().is_empty()
            || self.alarm_records.end_ts_input.trim().is_empty()
        {
            let end_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let start_ms = end_ms.saturating_sub(ALARM_RECORD_DEFAULT_WINDOW_MS);
            self.alarm_records.start_ts_input = format_datetime_input(start_ms);
            self.alarm_records.end_ts_input = format_datetime_input(end_ms);
        }
    }

    fn open_can_replay_from_alarm_records(&mut self) {
        let Some(data) = self.alarm_records.data.as_ref() else {
            self.alarm_records.status = "请先加载报警记录".to_string();
            return;
        };
        let Some(first_alarm_ts) = data.records.iter().map(|row| row.ts_ms).min() else {
            self.alarm_records.status = "当前页没有报警记录，无法打开时间分布".to_string();
            return;
        };
        let last_alarm_ts = data
            .records
            .iter()
            .map(|row| row.ts_ms)
            .max()
            .unwrap_or(first_alarm_ts);
        let replay_start_ts = first_alarm_ts.saturating_sub(ALARM_REPLAY_CONTEXT_MS);
        let replay_end_ts = last_alarm_ts.saturating_add(ALARM_REPLAY_CONTEXT_MS);

        self.can_replay.open = true;
        self.can_replay.start_ts_input = format_datetime_input(replay_start_ts);
        self.can_replay.end_ts_input = format_datetime_input(replay_end_ts);
        let has_can_group = self.alarm_records.show_can_x
            || self.alarm_records.show_can_y
            || self.alarm_records.show_can_z;
        self.can_replay.mode = if has_can_group {
            ReplayMode::Can3Axis
        } else {
            ReplayMode::Sent
        };
        self.can_replay.show_x = match self.can_replay.mode {
            ReplayMode::Can3Axis => self.alarm_records.show_can_x,
            ReplayMode::Sent => self.alarm_records.show_sent_t1,
        };
        self.can_replay.show_y = match self.can_replay.mode {
            ReplayMode::Can3Axis => self.alarm_records.show_can_y,
            ReplayMode::Sent => self.alarm_records.show_sent_t1,
        };
        self.can_replay.show_z = match self.can_replay.mode {
            ReplayMode::Can3Axis => self.alarm_records.show_can_z,
            ReplayMode::Sent => self.alarm_records.show_sent_t2,
        };
        self.can_replay.show_u = self.can_replay.mode == ReplayMode::Sent
            && self.alarm_records.show_sent_t2;
        self.can_replay.show_v = self.can_replay.mode == ReplayMode::Sent
            && self.alarm_records.show_sent_s;
        self.can_replay.data = None;
        self.can_replay.plot_rect = None;
        self.request_can_replay_load();
    }

    fn request_alarm_records_load(&mut self) {
        self.request_alarm_records_page(0);
    }

    fn request_alarm_records_page(&mut self, page_index: i64) {
        if self.alarm_records.loading {
            self.alarm_records.status = "正在加载，请稍候".to_string();
            return;
        }

        let start_ts_ms = match parse_time_input(&self.alarm_records.start_ts_input) {
            Some(v) => v,
            None => {
                self.alarm_records.status = "开始时间无效".to_string();
                return;
            }
        };
        let end_ts_ms = match parse_time_input(&self.alarm_records.end_ts_input) {
            Some(v) => v,
            None => {
                self.alarm_records.status = "结束时间无效".to_string();
                return;
            }
        };
        if end_ts_ms <= start_ts_ms {
            self.alarm_records.status = "结束时间必须大于开始时间".to_string();
            return;
        }

        let has_selected_group = self.alarm_records.show_can_x
            || self.alarm_records.show_can_y
            || self.alarm_records.show_can_z
            || self.alarm_records.show_sent_t1
            || self.alarm_records.show_sent_t2
            || self.alarm_records.show_sent_s;
        if !has_selected_group {
            self.alarm_records.status = "至少选择一个分组".to_string();
            return;
        }

        let page_index = page_index.max(0);
        let offset = page_index * ALARM_RECORD_PAGE_SIZE;
        let fetch_limit = ALARM_RECORD_PAGE_SIZE + 1;
        self.alarm_records.loading = true;
        self.alarm_records.status = format!("正在加载第 {} 页报警记录...", page_index + 1);

        let dsn = self.alarm_records.pg_dsn.clone();
        let tx = self.ui_tx.clone();
        let show_can_x = self.alarm_records.show_can_x;
        let show_can_y = self.alarm_records.show_can_y;
        let show_can_z = self.alarm_records.show_can_z;
        let show_sent_t1 = self.alarm_records.show_sent_t1;
        let show_sent_t2 = self.alarm_records.show_sent_t2;
        let show_sent_s = self.alarm_records.show_sent_s;
        thread::spawn(move || {
            let result = (|| -> Result<AlarmRecordData, String> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| err.to_string())?;
                rt.block_on(async move {
                    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
                        .await
                        .map_err(|err| format!("connect postgres failed: {err}; dsn={dsn}"))?;
                    tokio::spawn(async move {
                        let _ = connection.await;
                    });

                    let total_count: i64 = client
                        .query_one(
                            "SELECT COUNT(*)
                             FROM alarm_events
                             WHERE ts_ms >= $1
                               AND ts_ms <= $2
                               AND (($3 AND alarm_id IN ('can_x_h', 'can_x_l'))
                                 OR ($4 AND alarm_id IN ('can_y_h', 'can_y_l'))
                                 OR ($5 AND alarm_id IN ('can_z_h', 'can_z_l'))
                                 OR ($6 AND (alarm_id IN ('sent_torque_jump_t1', 'sent_angle_jump_t1') OR message LIKE '%t1=1%'))
                                 OR ($7 AND (alarm_id IN ('sent_torque_jump_t2', 'sent_angle_jump_t2') OR message LIKE '%t2=1%'))
                                 OR ($8 AND (alarm_id = 'sent_angle_jump_s' OR message LIKE '%s=1%')))",
                            &[
                                &start_ts_ms,
                                &end_ts_ms,
                                &show_can_x,
                                &show_can_y,
                                &show_can_z,
                                &show_sent_t1,
                                &show_sent_t2,
                                &show_sent_s,
                            ],
                        )
                        .await
                        .map_err(|err| err.to_string())?
                        .get(0);

                    let rows = client
                        .query(
                            "SELECT ts_ms, device_id, alarm_id, level, message, cleared
                             FROM alarm_events
                             WHERE ts_ms >= $1
                               AND ts_ms <= $2
                               AND (($3 AND alarm_id IN ('can_x_h', 'can_x_l'))
                                 OR ($4 AND alarm_id IN ('can_y_h', 'can_y_l'))
                                 OR ($5 AND alarm_id IN ('can_z_h', 'can_z_l'))
                                 OR ($6 AND (alarm_id IN ('sent_torque_jump_t1', 'sent_angle_jump_t1') OR message LIKE '%t1=1%'))
                                 OR ($7 AND (alarm_id IN ('sent_torque_jump_t2', 'sent_angle_jump_t2') OR message LIKE '%t2=1%'))
                                 OR ($8 AND (alarm_id = 'sent_angle_jump_s' OR message LIKE '%s=1%')))
                             ORDER BY ts_ms ASC, id ASC
                             LIMIT $9 OFFSET $10",
                            &[
                                &start_ts_ms,
                                &end_ts_ms,
                                &show_can_x,
                                &show_can_y,
                                &show_can_z,
                                &show_sent_t1,
                                &show_sent_t2,
                                &show_sent_s,
                                &fetch_limit,
                                &offset,
                            ],
                        )
                        .await
                        .map_err(|err| err.to_string())?;

                    let has_next = rows.len() as i64 > ALARM_RECORD_PAGE_SIZE;
                    let records = rows
                        .into_iter()
                        .take(ALARM_RECORD_PAGE_SIZE as usize)
                        .map(|row| AlarmRecordRow {
                            ts_ms: row.get(0),
                            device_id: row.get(1),
                            alarm_id: row.get(2),
                            level: row.get(3),
                            message: row.get(4),
                            cleared: row.get(5),
                        })
                        .collect();

                    Ok(AlarmRecordData {
                        records,
                        page_index,
                        has_next,
                        total_count,
                    })
                })
            })();

            let _ = tx.send(UiMsg::AlarmRecordsLoaded(result));
        });
    }

    fn alarm_record_matches_can_axis(row: &AlarmRecordRow, axis: &str) -> bool {
        row.alarm_id.starts_with(&format!("can_{axis}_"))
    }

    fn alarm_record_matches_sent_part(row: &AlarmRecordRow, part: &str) -> bool {
        match part {
            "t1" => row.alarm_id.ends_with("_t1") || row.message.contains("t1=1"),
            "t2" => row.alarm_id.ends_with("_t2") || row.message.contains("t2=1"),
            "s" => row.alarm_id.ends_with("_s") || row.message.contains("s=1"),
            _ => false,
        }
    }

    fn alarm_record_groups(row: &AlarmRecordRow) -> String {
        let mut groups = Vec::new();
        if Self::alarm_record_matches_can_axis(row, "x") {
            groups.push("CAN X");
        }
        if Self::alarm_record_matches_can_axis(row, "y") {
            groups.push("CAN Y");
        }
        if Self::alarm_record_matches_can_axis(row, "z") {
            groups.push("CAN Z");
        }
        if Self::alarm_record_matches_sent_part(row, "t1") {
            groups.push("SENT T1");
        }
        if Self::alarm_record_matches_sent_part(row, "t2") {
            groups.push("SENT T2");
        }
        if Self::alarm_record_matches_sent_part(row, "s") {
            groups.push("SENT S");
        }
        if groups.is_empty() {
            "Other".to_string()
        } else {
            groups.join(", ")
        }
    }

    fn alarm_record_level_color(level: &str) -> egui::Color32 {
        match level {
            "Purple" => egui::Color32::from_rgb(165, 88, 255),
            "Critical" => egui::Color32::from_rgb(220, 64, 52),
            "Warning" => egui::Color32::from_rgb(255, 180, 0),
            "Info" => egui::Color32::from_rgb(82, 170, 255),
            _ => egui::Color32::WHITE,
        }
    }

    fn draw_alarm_records_window(&mut self, ctx: &egui::Context) {
        if !self.alarm_records.open {
            return;
        }

        let mut open = self.alarm_records.open;
        egui::Window::new("报警记录")
            .open(&mut open)
            .default_size(egui::vec2(1040.0, 620.0))
            .min_width(820.0)
            .min_height(480.0)
            .show(ctx, |ui| {
                ui.horizontal_wrapped(|ui| {
                    ui.label("开始");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.alarm_records.start_ts_input)
                            .desired_width(172.0),
                    );
                    ui.label("结束");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.alarm_records.end_ts_input)
                            .desired_width(172.0),
                    );
                    if ui
                        .add_enabled(!self.alarm_records.loading, egui::Button::new("加载"))
                        .clicked()
                    {
                        self.request_alarm_records_load();
                    }
                    if ui.button("最近5分钟").clicked() {
                        let end_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);
                        self.alarm_records.start_ts_input =
                            format_datetime_input(end_ms.saturating_sub(ALARM_RECORD_DEFAULT_WINDOW_MS));
                        self.alarm_records.end_ts_input = format_datetime_input(end_ms);
                        self.alarm_records.page_index = 0;
                        self.alarm_records.has_next = false;
                        self.alarm_records.data = None;
                    }
                });

                ui.add_space(8.0);
                ui.horizontal_wrapped(|ui| {
                    ui.label("分组");
                    ui.toggle_value(&mut self.alarm_records.show_can_x, "CAN X");
                    ui.toggle_value(&mut self.alarm_records.show_can_y, "CAN Y");
                    ui.toggle_value(&mut self.alarm_records.show_can_z, "CAN Z");
                    ui.separator();
                    ui.toggle_value(&mut self.alarm_records.show_sent_t1, "SENT T1");
                    ui.toggle_value(&mut self.alarm_records.show_sent_t2, "SENT T2");
                    ui.toggle_value(&mut self.alarm_records.show_sent_s, "SENT S");
                });

                ui.horizontal_wrapped(|ui| {
                    if ui
                        .add_enabled(
                            !self.alarm_records.loading && self.alarm_records.page_index > 0,
                            egui::Button::new("上一页"),
                        )
                        .clicked()
                    {
                        self.request_alarm_records_page(self.alarm_records.page_index - 1);
                    }
                    ui.label(format!("第 {} 页", self.alarm_records.page_index + 1));
                    if ui
                        .add_enabled(
                            !self.alarm_records.loading && self.alarm_records.has_next,
                            egui::Button::new("下一页"),
                        )
                        .clicked()
                    {
                        self.request_alarm_records_page(self.alarm_records.page_index + 1);
                    }
                    ui.separator();
                    if ui.button("时间分布").clicked() {
                        self.open_can_replay_from_alarm_records();
                    }
                });

                ui.label(&self.alarm_records.status);
                ui.small(format!(
                    "按时间顺序显示，每页 {} 条，读取 alarm_events",
                    ALARM_RECORD_PAGE_SIZE
                ));
                ui.add_space(6.0);

                if let Some(data) = self.alarm_records.data.clone() {
                    ui.label(format!("本页 {} 条记录", data.records.len()));
                    ui.separator();
                    if data.records.is_empty() {
                        ui.vertical_centered(|ui| {
                            ui.add_space(80.0);
                            ui.heading("没有报警记录");
                        });
                    } else {
                        egui::ScrollArea::vertical()
                            .id_salt("alarm_records_db_scroll")
                            .show(ui, |ui| {
                                for row in &data.records {
                                    let color = Self::alarm_record_level_color(&row.level);
                                    ui.horizontal_wrapped(|ui| {
                                        ui.label(format_datetime_input(row.ts_ms));
                                        ui.colored_label(color, &row.level);
                                        ui.label(if row.cleared { "已清除" } else { "已触发" });
                                        ui.label(Self::alarm_record_groups(row));
                                        ui.label(&row.device_id);
                                    });
                                    ui.label(
                                        egui::RichText::new(row.alarm_id.as_str())
                                            .monospace()
                                            .color(color),
                                    );
                                    ui.colored_label(color, &row.message);
                                    ui.separator();
                                }
                            });
                    }
                } else {
                    ui.group(|ui| {
                        ui.set_min_height(260.0);
                        ui.vertical_centered(|ui| {
                            ui.add_space(92.0);
                            ui.heading("报警记录");
                            ui.label("选择时间范围后点击加载");
                        });
                    });
                }
            });
        self.alarm_records.open = open;
    }

    fn send_can_self_test(&mut self) {
        self.can_self_test_counter = self.can_self_test_counter.wrapping_add(1);
        let data = SELF_TEST_CAN_DATA;
        let frame = CanTxFrame::new(0, SELF_TEST_CAN_ID, SELF_TEST_CAN_DLC, data);
        match enqueue_can_tx(frame) {
            Ok(()) => {
                let message = format!(
                    "CAN self-test queued: id=0x{:X} dlc={} data={:02X?}，等待 collector 回报自检结果",
                    SELF_TEST_CAN_ID, SELF_TEST_CAN_DLC, data
                );
                self.last_can_self_test_result = message.clone();
                self.status = message;
            }
            Err(err) => {
                let message = format!("CAN self-test failed: {err}");
                self.last_can_self_test_result = message.clone();
                self.status = message;
            }
        }
    }
}

impl eframe::App for UiClientApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_messages();
        self.apply_ctrl_wheel_zoom(ctx);

        egui::TopBottomPanel::top("self_test_result_top")
            .resizable(false)
            .default_height(24.0)
            .show(ctx, |ui| {
                ui.label(format!("最近自检结果: {}", self.last_can_self_test_result));
            });

        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.heading("UI Client");
            ui.label(format!("collector feed: {}", self.feed_addr));
            ui.label("serial data path: serial -> collector_service -> ui_client");
            ui.horizontal(|ui| {
                if ui.button("重排窗口并重置缩放").clicked() {
                    self.reset_layout();
                }
                if ui
                    .add_enabled(
                        !self.dynamic_windows.is_empty(),
                        egui::Button::new("删除全部显示框"),
                    )
                    .clicked()
                {
                    self.dynamic_windows.clear();
                }
                if ui.button("CAN自检").clicked() {
                    self.send_can_self_test();
                }
                if ui.button("CAN回放").clicked() {
                    self.open_can_replay();
                }
                if ui.button("报警记录").clicked() {
                    self.open_alarm_records();
                }
            });
            ui.horizontal(|ui| {
                ui.label("Test signal");
                ui.selectable_value(&mut self.selected_view, TestSignalView::Demo, "DEMO");
                ui.selectable_value(&mut self.selected_view, TestSignalView::Sent, "SENT");
                ui.selectable_value(&mut self.selected_view, TestSignalView::CanFrame, "CAN");
                ui.selectable_value(&mut self.selected_view, TestSignalView::TcpFrame, "TCP");
                if ui.button("Add").clicked() {
                    self.add_dynamic_window();
                }
            });
            ui.label(format!("状态: {}", self.status));
            ui.label(format!("总样本数: {}", self.total_samples));
            ui.label(format!(
                "丢弃的样本(ui队列满): {}",
                self.feed_stats.dropped_samples.load(Ordering::Relaxed)
            ));
            ui.label(format!(
                "解码失败数: {}",
                self.feed_stats.decode_errors.load(Ordering::Relaxed)
            ));
            ui.label(format!("最新请求ID: {}", self.last_req));
        });

        self.draw_alarm_panel(ctx);
        self.draw_demo_alarm_indicator(ctx);
        self.draw_sent_alarm_indicator(ctx);
        self.draw_sent_jump_indicator(ctx);
        self.draw_sent_jump_threshold_panel(ctx);
        self.draw_sent_angle_jump_indicator(ctx);
        self.draw_sent_angle_jump_threshold_panel(ctx);
        self.draw_can_alarm_indicator(ctx);
        self.draw_can_threshold_panel(ctx);
        self.draw_can_replay_window(ctx);
        self.draw_alarm_records_window(ctx);

        let mut remove_idx = Vec::new();
        for idx in 0..self.dynamic_windows.len() {
            let title = self.dynamic_windows[idx].title.clone();
            let binding = self.dynamic_windows[idx].binding;
            let start_pos = self.dynamic_windows[idx].position;
            let scale = self.dynamic_windows[idx].scale;
            let id = egui::Id::new(format!("dynamic_signal_window_{idx}"));
            let mut current_pos = start_pos;
            let mut open = true;
            let win_w = 290.0 * scale;
            let win_h = 220.0 * scale;
            let chart_h = 120.0 * scale;
            let response = egui::Window::new(title)
                .id(id)
                .open(&mut open)
                .current_pos(start_pos)
                .movable(true)
                .resizable(false)
                .collapsible(false)
                .fixed_size(egui::vec2(win_w, win_h))
                .show(ctx, |ui| {
                    if let Some(binding) = binding {
                        let sensor_id = binding.sensor_id();
                        if sensor_id >= SENSOR_COUNT {
                            ui.label(format!("sensor {sensor_id} 超出当前可用范围"));
                            return;
                        }
                        let raw_signal_id = format!("sensor_{sensor_id}_raw");
                        let series = if binding.uses_tcp_series() {
                            &self.tcp_sensors[sensor_id]
                        } else {
                            &self.sensors[sensor_id]
                        };
                        if let Some(v) = series.latest {
                            let text = if binding.is_can_axis() {
                                format!("{:.0}", v)
                            } else {
                                self.signal_value_text(&raw_signal_id, v)
                            };
                            ui.label(format!("{}: {}", binding.value_label(), text));
                        } else {
                            ui.label(format!("{}: N/A", binding.value_label()));
                        }
                        if let Some(text) =
                            self.latest_demo_derived_angle_text(binding, &series.device_id)
                        {
                            ui.label(format!("angle: {}", text));
                        }
                        if !series.device_id.is_empty() {
                            ui.label(format!("device: {}", series.device_id));
                        }
                        let chart_label = binding
                            .chart_label()
                            .map(str::to_string)
                            .unwrap_or_else(|| self.sensor_label(sensor_id));
                        let chart_thresholds = self.can_chart_thresholds(binding);
                        Self::draw_sensor_chart(
                            ui,
                            &series.points,
                            chart_h,
                            &format!("{chart_label} (last {:.0}s)", WINDOW_SECS),
                            chart_thresholds,
                        );
                    } else {
                        ui.label("No signal mapping for this mode yet.");
                    }
                });

            if let Some(inner) = response {
                current_pos = inner.response.rect.min;
                self.dynamic_windows[idx].rect = Some(inner.response.rect);
            } else {
                self.dynamic_windows[idx].rect = None;
            }
            self.dynamic_windows[idx].position = current_pos;
            if !open {
                remove_idx.push(idx);
            }
        }

        for idx in remove_idx.into_iter().rev() {
            self.dynamic_windows.remove(idx);
        }

        ctx.request_repaint_after(Duration::from_millis(16));
    }
}

fn main() -> eframe::Result<()> {
    let (tx, rx) = mpsc::sync_channel::<UiMsg>(UI_QUEUE_CAPACITY);
    let feed_stats = Arc::new(FeedStats::default());
    let feed_addr = load_feed_addr();
    let pg_dsn = load_pg_dsn();
    let ui_tx = tx.clone();

    thread::spawn(|| {
        let rt = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(err) => {
                eprintln!("embedded collector runtime init failed: {err}");
                return;
            }
        };

        if let Err(err) = rt.block_on(embedded_collector_service::run()) {
            eprintln!("embedded collector stopped: {err}");
        }
    });

    let feed_stats_for_thread = feed_stats.clone();
    let feed_addr_for_thread = feed_addr.clone();
    thread::spawn(move || {
        resilient_feed_thread(feed_addr_for_thread, tx, feed_stats_for_thread);
    });

    let options = eframe::NativeOptions::default();
    let app_creator = move |cc: &eframe::CreationContext<'_>| -> Result<
        Box<dyn eframe::App>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        setup_chinese_fonts(&cc.egui_ctx);
        cc.egui_ctx
            .options_mut(|options| options.warn_on_id_clash = false);
        Ok(Box::new(UiClientApp::new(
            rx,
            ui_tx.clone(),
            feed_stats,
            feed_addr,
            pg_dsn,
        )))
    };

    eframe::run_native("demo2_ui_client", options, Box::new(app_creator))
}
