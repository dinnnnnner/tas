use demo2::bus::{AppEvent, DeviceEvent, EventBus, Store, TelemetrySourceKind};
use demo2::app::AlarmService;
use demo2::db::SCHEMA_SQL;
use demo2::domain::AlarmEvent;
use demo2::ingress::can::{SentFilterConfig, run_can_ingress};
use demo2::ingress::serial::{
    SerialIngressMode, parse_serial_mode, publish_status, run_serial_ingress,
};
use demo2::ingress::tcp::run_tcp_ingress;
use demo2::protocol::SimpleFrameCodec;
use demo2::session::{DeviceSession, DeviceSessionHandle, SessionConfig};
use demo2::transport::can::CanTransportConfig;
use demo2::transport::SerialTransport;
use biquad::{Biquad, Coefficients, DirectForm1, ToHertz, Type};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::sync::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio_postgres::NoTls;
use tracing::{info, warn};

const DB_CMD_CHANNEL_CAPACITY: usize = 50_000;
const TELEMETRY_BATCH_SIZE: usize = 256;
const DEMO_UI_STRIDE: u64 = 8;
const DEFAULT_DB_FILTER_ORDER: usize = 10;
const DEFAULT_DB_FILTER_SAMPLE_RATE_HZ: f32 = 48_000.0;
const DEFAULT_DB_FILTER_CUTOFF_HZ: f32 = 4_000.0;
const DEFAULT_SENT_FILTER_WINDOW: usize = 10;

fn default_db_filter_enabled() -> bool {
    false
}

fn default_db_filter_order() -> usize {
    DEFAULT_DB_FILTER_ORDER
}

fn default_db_filter_sample_rate_hz() -> f32 {
    DEFAULT_DB_FILTER_SAMPLE_RATE_HZ
}

fn default_db_filter_cutoff_hz() -> f32 {
    DEFAULT_DB_FILTER_CUTOFF_HZ
}

fn default_sent_filter_enabled() -> bool {
    true
}

fn default_sent_filter_window() -> usize {
    DEFAULT_SENT_FILTER_WINDOW
}

fn default_can_autostart_tsmaster() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
struct CollectorConfig {
    ingress_addr: String,
    ui_feed_addr: String,
    health_addr: String,
    can_enabled: bool,
    can_tsmaster_bin: Option<String>,
    #[serde(default = "default_can_autostart_tsmaster")]
    can_autostart_tsmaster: bool,
    can_hardware_name: String,
    can_channel: u8,
    can_baud_kbps: u32,
    can_data_baud_kbps: u32,
    serial_port: Option<String>,
    serial_baud: u32,
    serial_mode: String,
    pg_dsn: String,
    pg_connect_max_retries: u32,
    pg_connect_retry_ms: u64,
    #[serde(default = "default_db_filter_enabled")]
    db_filter_enabled: bool,
    #[serde(default = "default_db_filter_order")]
    db_filter_order: usize,
    #[serde(default = "default_db_filter_sample_rate_hz")]
    db_filter_sample_rate_hz: f32,
    #[serde(default = "default_db_filter_cutoff_hz")]
    db_filter_cutoff_hz: f32,
    #[serde(default = "default_sent_filter_enabled")]
    sent_filter_enabled: bool,
    #[serde(default = "default_sent_filter_window")]
    sent_filter_window: usize,
    max_payload: usize,
    bus_capacity: usize,
    ui_feed_capacity: usize,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            ingress_addr: "127.0.0.1:19010".to_string(),
            ui_feed_addr: "127.0.0.1:19011".to_string(),
            health_addr: "127.0.0.1:19012".to_string(),
            can_enabled: false,
            can_tsmaster_bin: None,
            can_autostart_tsmaster: true,
            can_hardware_name: "TC1012".to_string(),
            can_channel: 0,
            can_baud_kbps: 500,
            can_data_baud_kbps: 2_000,
            serial_port: None,
            serial_baud: 2_000_000,
            serial_mode: "sent".to_string(),
            pg_dsn: "host=127.0.0.1 port=5432 user=postgres password=123456 dbname=demo2"
                .to_string(),
            pg_connect_max_retries: 20,
            pg_connect_retry_ms: 1000,
            db_filter_enabled: false,
            db_filter_order: DEFAULT_DB_FILTER_ORDER,
            db_filter_sample_rate_hz: DEFAULT_DB_FILTER_SAMPLE_RATE_HZ,
            db_filter_cutoff_hz: DEFAULT_DB_FILTER_CUTOFF_HZ,
            sent_filter_enabled: true,
            sent_filter_window: DEFAULT_SENT_FILTER_WINDOW,
            max_payload: 4096,
            bus_capacity: 10_000,
            ui_feed_capacity: 10_000,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    collector: Option<CollectorConfig>,
}

fn load_config() -> CollectorConfig {
    let path = "config.toml";
    let Ok(text) = fs::read_to_string(path) else {
        return CollectorConfig::default();
    };

    match toml::from_str::<ConfigFile>(&text) {
        Ok(file) => file.collector.unwrap_or_default(),
        Err(err) => {
            warn!(error = %err, "failed to parse config.toml, fallback to defaults");
            CollectorConfig::default()
        }
    }
}

fn env_flag(name: &str) -> Option<bool> {
    std::env::var(name).ok().and_then(|v| match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    })
}

#[derive(Default)]
struct CollectorStats {
    ingress_ready: AtomicBool,
    ui_ready: AtomicBool,
    ingress_connections: AtomicU64,
    ui_clients: AtomicU64,
    samples_rx: AtomicU64,
    ui_drop: AtomicU64,
    db_drop: AtomicU64,
    db_write_fail: AtomicU64,
    last_db_error: Mutex<Option<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TelemetryMsg {
    pub device_id: String,
    pub sensor_id: usize,
    pub axis: String,
    pub alarm_bit: bool,
    pub t_sec: f64,
    pub value: f64,
    pub request_id: u64,
    pub source_kind: TelemetrySourceKind,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
enum UiFeedMsg {
    Telemetry(TelemetryMsg),
    Alarm(AlarmEvent),
    Status(String),
}

enum DbCmd {
    Telemetry(TelemetryMsg),
    Alarm(AlarmEvent),
    System { level: String, message: String },
}

struct ButterworthCascade {
    sections: Vec<DirectForm1<f32>>,
}

impl ButterworthCascade {
    fn new(order: usize, sample_rate_hz: f32, cutoff_hz: f32) -> anyhow::Result<Self> {
        anyhow::ensure!(order >= 2, "db_filter_order must be at least 2");
        anyhow::ensure!(
            order % 2 == 0,
            "db_filter_order must be even for cascaded Butterworth sections"
        );
        anyhow::ensure!(sample_rate_hz > 0.0, "db_filter_sample_rate_hz must be positive");
        anyhow::ensure!(cutoff_hz > 0.0, "db_filter_cutoff_hz must be positive");
        anyhow::ensure!(
            cutoff_hz < sample_rate_hz * 0.5,
            "db_filter_cutoff_hz must be below Nyquist"
        );

        let section_count = order / 2;
        let mut sections = Vec::with_capacity(section_count);

        for index in 1..=section_count {
            let q = butterworth_section_q(order, index);
            let coeffs = Coefficients::<f32>::from_params(
                Type::LowPass,
                sample_rate_hz.hz(),
                cutoff_hz.hz(),
                q,
            )
            .map_err(|err| anyhow::anyhow!("build biquad coeffs failed: {err:?}"))?;
            sections.push(DirectForm1::<f32>::new(coeffs));
        }

        Ok(Self { sections })
    }

    fn run(&mut self, mut sample: f64) -> f64 {
        let mut x = sample as f32;
        for section in &mut self.sections {
            x = section.run(x);
        }
        sample = x as f64;
        sample
    }
}

fn butterworth_section_q(order: usize, section_index: usize) -> f32 {
    let theta = ((2 * section_index - 1) as f32 * std::f32::consts::PI) / (2.0 * order as f32);
    1.0 / (2.0 * theta.cos())
}

#[derive(Clone, Copy)]
struct PersistenceFilterConfig {
    enabled: bool,
    order: usize,
    sample_rate_hz: f32,
    cutoff_hz: f32,
}

struct PersistenceFilter {
    config: PersistenceFilterConfig,
    filters: HashMap<(String, usize, String), ButterworthCascade>,
    init_error_logged: bool,
}

impl PersistenceFilter {
    fn new(config: PersistenceFilterConfig) -> Self {
        Self {
            config,
            filters: HashMap::new(),
            init_error_logged: false,
        }
    }

    fn apply(&mut self, mut msg: TelemetryMsg) -> TelemetryMsg {
        if !self.config.enabled {
            return msg;
        }

        let key = (msg.device_id.clone(), msg.sensor_id, msg.axis.clone());
        match self.filters.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                msg.value = entry.get_mut().run(msg.value);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                match ButterworthCascade::new(
                    self.config.order,
                    self.config.sample_rate_hz,
                    self.config.cutoff_hz,
                ) {
                    Ok(mut filter) => {
                        msg.value = filter.run(msg.value);
                        entry.insert(filter);
                    }
                    Err(err) => {
                        if !self.init_error_logged {
                            warn!(error = %err, "db Butterworth filter init failed, bypassing filter");
                            self.init_error_logged = true;
                        }
                    }
                }
            }
        }

        msg
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn record_db_error(stats: &CollectorStats, message: String) {
    if let Ok(mut guard) = stats.last_db_error.lock() {
        *guard = Some(message);
    }
}

async fn ensure_telemetry_partitions(
    client: &tokio_postgres::Client,
    ts_values: &[i64],
) -> anyhow::Result<()> {
    let mut days = ts_values
        .iter()
        .map(|ts_ms| ts_ms.div_euclid(86_400_000))
        .collect::<Vec<_>>();
    days.sort_unstable();
    days.dedup();

    for day in days {
        let day_start_ms = (day * 86_400_000) as f64;
        client
            .execute(
                "SELECT ensure_telemetry_partition_for_day(to_timestamp($1::double precision / 1000.0)::date)",
                &[&day_start_ms],
            )
            .await
            .map_err(|err| anyhow::anyhow!("ensure telemetry partition failed: {err}"))?;
    }

    Ok(())
}

fn axis_name(source_kind: TelemetrySourceKind, sensor_id: usize) -> &'static str {
    match source_kind {
        TelemetrySourceKind::SerialDemo => match sensor_id {
            0 => "x",
            1 => "y",
            2 => "z",
            _ => "",
        },
        TelemetrySourceKind::CanAxis => match sensor_id {
            0 => "x",
            1 => "y",
            2 => "z",
            _ => "",
        },
        TelemetrySourceKind::CanSent => match sensor_id {
            0 => "t1_angle",
            1 => "t1_torque",
            2 => "t2_angle",
            3 => "t2_torque",
            4 => "s_angle",
            _ => "",
        },
        _ => "",
    }
}

async fn flush_telemetry_batch(
    client: &tokio_postgres::Client,
    batch: &mut Vec<TelemetryMsg>,
    stats: &CollectorStats,
) {
    if batch.is_empty() {
        return;
    }

    let mut ts_ms = Vec::with_capacity(batch.len());
    let mut device_ids = Vec::with_capacity(batch.len());
    let mut sensor_ids = Vec::with_capacity(batch.len());
    let mut axes = Vec::with_capacity(batch.len());
    let mut alarm_bits = Vec::with_capacity(batch.len());
    let mut t_secs = Vec::with_capacity(batch.len());
    let mut values = Vec::with_capacity(batch.len());
    let mut request_ids = Vec::with_capacity(batch.len());

    for item in batch.drain(..) {
        let row_ts_ms = now_ms();
        ts_ms.push(row_ts_ms);
        device_ids.push(item.device_id);
        sensor_ids.push(item.sensor_id as i32);
        axes.push(item.axis);
        alarm_bits.push(item.alarm_bit);
        t_secs.push(item.t_sec);
        values.push(item.value);
        request_ids.push(item.request_id as i64);
    }

    if let Err(err) = ensure_telemetry_partitions(client, &ts_ms).await {
        stats.db_write_fail.fetch_add(1, Ordering::Relaxed);
        record_db_error(stats, format!("telemetry_partition: {err}"));
        warn!(error = %err, batch_len = ts_ms.len(), op = "telemetry_partition", "postgres partition ensure failed");
        return;
    }

    if let Err(err) = client
        .execute(
            "INSERT INTO telemetry_samples (ts_ms, created_at, device_id, sensor_id, axis, alarm_bit, t_sec, value, request_id)
             SELECT
                unnest_ts_ms,
                to_timestamp(unnest_ts_ms::double precision / 1000.0),
                unnest_device_id,
                unnest_sensor_id,
                unnest_axis,
                unnest_alarm_bit,
                unnest_t_sec,
                unnest_value,
                unnest_request_id
             FROM UNNEST(
                $1::BIGINT[],
                $2::TEXT[],
                $3::INTEGER[],
                $4::TEXT[],
                $5::BOOLEAN[],
                $6::DOUBLE PRECISION[],
                $7::DOUBLE PRECISION[],
                $8::BIGINT[]
             ) AS t(
                unnest_ts_ms,
                unnest_device_id,
                unnest_sensor_id,
                unnest_axis,
                unnest_alarm_bit,
                unnest_t_sec,
                unnest_value,
                unnest_request_id
             )",
            &[
                &ts_ms,
                &device_ids,
                &sensor_ids,
                &axes,
                &alarm_bits,
                &t_secs,
                &values,
                &request_ids,
            ],
        )
        .await
    {
        stats.db_write_fail.fetch_add(1, Ordering::Relaxed);
        record_db_error(stats, format!("telemetry_batch: {err}"));
        warn!(error = %err, batch_len = ts_ms.len(), op = "telemetry_batch", "postgres write failed");
    }
}

async fn start_pg_writer(
    cfg: &CollectorConfig,
    stats: Arc<CollectorStats>,
) -> anyhow::Result<mpsc::Sender<DbCmd>> {
    let mut attempt = 0_u32;
    let (client, connection) = loop {
        attempt = attempt.saturating_add(1);
        match tokio_postgres::connect(&cfg.pg_dsn, NoTls).await {
            Ok(ok) => {
                info!("postgres connected on attempt {}", attempt);
                break ok;
            }
            Err(err) => {
                if attempt >= cfg.pg_connect_max_retries {
                    return Err(anyhow::anyhow!(
                        "postgres connect failed after {} attempts: {}",
                        attempt,
                        err
                    ));
                }
                warn!(
                    attempt,
                    max_attempts = cfg.pg_connect_max_retries,
                    retry_ms = cfg.pg_connect_retry_ms,
                    error = %err,
                    "postgres connect failed, retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(cfg.pg_connect_retry_ms)).await;
            }
        }
    };

    tokio::spawn(async move {
        if let Err(err) = connection.await {
            warn!(error = %err, "postgres connection task ended");
        }
    });

    client
        .batch_execute(SCHEMA_SQL)
        .await
        .map_err(|err| anyhow::anyhow!("postgres schema init failed: {err}"))?;

    let (tx, mut rx) = mpsc::channel::<DbCmd>(DB_CMD_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let mut telemetry_batch = Vec::with_capacity(TELEMETRY_BATCH_SIZE);
        while let Some(cmd) = rx.recv().await {
            match cmd {
                DbCmd::Telemetry(t) => {
                    telemetry_batch.push(t);
                    while telemetry_batch.len() < TELEMETRY_BATCH_SIZE {
                        match rx.try_recv() {
                            Ok(DbCmd::Telemetry(t)) => telemetry_batch.push(t),
                            Ok(other) => {
                                flush_telemetry_batch(&client, &mut telemetry_batch, &stats).await;
                                let (res, op) = match other {
                                    DbCmd::Alarm(a) => {
                                        let ts_ms = now_ms();
                                        (
                                            client
                                                .execute(
                                                    "INSERT INTO alarm_events (ts_ms, device_id, alarm_id, level, message, cleared)
                                                     VALUES ($1, $2, $3, $4, $5, $6)",
                                                    &[
                                                        &ts_ms,
                                                        &a.device_id,
                                                        &a.alarm_id,
                                                        &format!("{:?}", a.level),
                                                        &a.message,
                                                        &a.cleared,
                                                    ],
                                                )
                                                .await,
                                            "alarm",
                                        )
                                    }
                                    DbCmd::System { level, message } => {
                                        let ts_ms = now_ms();
                                        (
                                            client
                                                .execute(
                                                    "INSERT INTO system_events (ts_ms, level, message)
                                                     VALUES ($1, $2, $3)",
                                                    &[&ts_ms, &level, &message],
                                                )
                                                .await,
                                            "system",
                                        )
                                    }
                                    DbCmd::Telemetry(_) => unreachable!(),
                                };
                                if let Err(err) = res {
                                    stats.db_write_fail.fetch_add(1, Ordering::Relaxed);
                                    record_db_error(&stats, format!("{op}: {err}"));
                                    warn!(error = %err, op, "postgres write failed");
                                }
                                continue;
                            }
                            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                        }
                    }
                    flush_telemetry_batch(&client, &mut telemetry_batch, &stats).await;
                }
                DbCmd::Alarm(a) => {
                    flush_telemetry_batch(&client, &mut telemetry_batch, &stats).await;
                    let ts_ms = now_ms();
                    if let Err(err) = client
                        .execute(
                            "INSERT INTO alarm_events (ts_ms, device_id, alarm_id, level, message, cleared)
                             VALUES ($1, $2, $3, $4, $5, $6)",
                            &[
                                &ts_ms,
                                &a.device_id,
                                &a.alarm_id,
                                &format!("{:?}", a.level),
                                &a.message,
                                &a.cleared,
                            ],
                        )
                        .await
                    {
                        stats.db_write_fail.fetch_add(1, Ordering::Relaxed);
                        record_db_error(&stats, format!("alarm: {err}"));
                        warn!(error = %err, op = "alarm", "postgres write failed");
                    }
                }
                DbCmd::System { level, message } => {
                    flush_telemetry_batch(&client, &mut telemetry_batch, &stats).await;
                    let ts_ms = now_ms();
                    if let Err(err) = client
                        .execute(
                            "INSERT INTO system_events (ts_ms, level, message)
                             VALUES ($1, $2, $3)",
                            &[&ts_ms, &level, &message],
                        )
                        .await
                    {
                        stats.db_write_fail.fetch_add(1, Ordering::Relaxed);
                        record_db_error(&stats, format!("system: {err}"));
                        warn!(error = %err, op = "system", "postgres write failed");
                    }
                }
            }
        }
        flush_telemetry_batch(&client, &mut telemetry_batch, &stats).await;
    });

    Ok(tx)
}

async fn serve_ui_client(
    mut socket: TcpStream,
    mut rx: broadcast::Receiver<String>,
    stats: Arc<CollectorStats>,
) {
    stats.ui_clients.fetch_add(1, Ordering::Relaxed);
    loop {
        let line = match rx.recv().await {
            Ok(line) => line,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(skipped, "ui feed lagged, skipping stale messages");
                stats.ui_drop.fetch_add(skipped as u64, Ordering::Relaxed);
                continue;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        };

        if socket.write_all(line.as_bytes()).await.is_err() {
            break;
        }
        if socket.write_all(b"\n").await.is_err() {
            break;
        }
    }
    stats.ui_clients.fetch_sub(1, Ordering::Relaxed);
}

async fn run_ui_forwarder(
    mut sub: tokio::sync::broadcast::Receiver<AppEvent>,
    ui_tx: broadcast::Sender<String>,
    stats: Arc<CollectorStats>,
) {
    let mut demo_emit_counters: HashMap<(String, usize), u64> = HashMap::new();
    loop {
        match sub.recv().await {
            Ok(AppEvent::Device(DeviceEvent::TelemetrySample {
                device_id,
                sensor_id,
                t_sec,
                value,
                req_id,
                alarm_bit,
                source_kind,
            })) => {
                let msg = TelemetryMsg {
                    device_id,
                    sensor_id,
                    axis: axis_name(source_kind, sensor_id).to_string(),
                    alarm_bit,
                    t_sec,
                    value,
                    request_id: req_id,
                    source_kind,
                };
                if msg.source_kind == TelemetrySourceKind::SerialDemo {
                    let key = (msg.device_id.clone(), msg.sensor_id);
                    let counter = demo_emit_counters.entry(key).or_insert(0);
                    *counter = counter.saturating_add(1);
                    if *counter % DEMO_UI_STRIDE != 0 {
                        continue;
                    }
                }
                if let Ok(line) = serde_json::to_string(&UiFeedMsg::Telemetry(msg)) {
                    if ui_tx.send(line).is_err() {
                        stats.ui_drop.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Ok(AppEvent::Device(DeviceEvent::AlarmRaised(alarm)))
            | Ok(AppEvent::Device(DeviceEvent::AlarmCleared(alarm))) => {
                if let Ok(line) = serde_json::to_string(&UiFeedMsg::Alarm(alarm)) {
                    if ui_tx.send(line).is_err() {
                        stats.ui_drop.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Ok(AppEvent::System(msg)) => {
                if let Ok(line) = serde_json::to_string(&UiFeedMsg::Status(msg)) {
                    if ui_tx.send(line).is_err() {
                        stats.ui_drop.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
}

async fn run_alarm_forwarder(
    mut sub: tokio::sync::broadcast::Receiver<AppEvent>,
    bus: EventBus,
    stats: Arc<CollectorStats>,
) {
    let alarm_service = AlarmService::new(bus);
    let mut active_sessions: std::collections::HashSet<String> = std::collections::HashSet::new();
    loop {
        match sub.recv().await {
            Ok(AppEvent::Device(DeviceEvent::TelemetrySample {
                device_id,
                sensor_id,
                value,
                ..
            })) => {
                stats.samples_rx.fetch_add(1, Ordering::Relaxed);
                alarm_service.evaluate_sample(&device_id, sensor_id, value);
            }
            Ok(AppEvent::Device(DeviceEvent::ConnStateChanged { device_id, to, .. })) => {
                match to {
                    demo2::domain::ConnState::Ready => {
                        active_sessions.insert(device_id);
                    }
                    demo2::domain::ConnState::Disconnected => {
                        active_sessions.remove(&device_id);
                    }
                    _ => {}
                }
                stats
                    .ingress_connections
                    .store(active_sessions.len() as u64, Ordering::Relaxed);
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
}

async fn run_persistence_forwarder(
    mut sub: tokio::sync::broadcast::Receiver<AppEvent>,
    db_tx: mpsc::Sender<DbCmd>,
    stats: Arc<CollectorStats>,
) {
    loop {
        match sub.recv().await {
            Ok(AppEvent::Device(DeviceEvent::TelemetrySample {
                device_id,
                sensor_id,
                t_sec,
                value,
                req_id,
                alarm_bit,
                source_kind,
            })) => {
                let msg = TelemetryMsg {
                    device_id,
                    sensor_id,
                    axis: axis_name(source_kind, sensor_id).to_string(),
                    alarm_bit,
                    t_sec,
                    value,
                    request_id: req_id,
                    source_kind,
                };
                let _ = db_tx
                    .send(DbCmd::Telemetry(msg))
                    .await;
            }
            Ok(AppEvent::Device(DeviceEvent::AlarmRaised(alarm))) => {
                let _ = db_tx.send(DbCmd::Alarm(alarm)).await;
            }
            Ok(AppEvent::Device(DeviceEvent::AlarmCleared(alarm))) => {
                let _ = db_tx.send(DbCmd::Alarm(alarm)).await;
            }
            Ok(AppEvent::System(msg)) => {
                let _ = db_tx
                    .send(DbCmd::System {
                        level: "info".to_string(),
                        message: msg,
                    })
                    .await;
            }
            Ok(AppEvent::Device(DeviceEvent::Log { level, msg, .. })) => {
                let _ = db_tx
                    .send(DbCmd::System {
                        level: level.to_string(),
                        message: msg,
                    })
                    .await;
            }
            Ok(_) => {}
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(skipped, "persistence lagged, skipping stale messages");
                stats.db_drop.fetch_add(skipped as u64, Ordering::Relaxed);
                continue;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

async fn run_filtered_event_forwarder(
    mut sub: tokio::sync::broadcast::Receiver<AppEvent>,
    processed_bus: EventBus,
    filter_config: PersistenceFilterConfig,
) {
    let mut filter = PersistenceFilter::new(filter_config);
    loop {
        match sub.recv().await {
            Ok(AppEvent::Device(DeviceEvent::TelemetrySample {
                device_id,
                sensor_id,
                t_sec,
                value,
                req_id,
                alarm_bit,
                source_kind,
            })) => {
                let filtered = filter.apply(TelemetryMsg {
                    device_id,
                    sensor_id,
                    axis: axis_name(source_kind, sensor_id).to_string(),
                    alarm_bit,
                    t_sec,
                    value,
                    request_id: req_id,
                    source_kind,
                });
                processed_bus.publish(AppEvent::Device(DeviceEvent::TelemetrySample {
                    device_id: filtered.device_id,
                    sensor_id: filtered.sensor_id,
                    t_sec: filtered.t_sec,
                    value: filtered.value,
                    req_id: filtered.request_id,
                    alarm_bit: filtered.alarm_bit,
                    source_kind: filtered.source_kind,
                }));
            }
            Ok(other) => processed_bus.publish(other),
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(skipped, "filter forwarder lagged, skipping stale messages");
                continue;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

fn health_json(stats: &CollectorStats) -> String {
    let last_db_error = stats
        .last_db_error
        .lock()
        .ok()
        .and_then(|guard| guard.clone());
    serde_json::json!({
        "ingress_ready": stats.ingress_ready.load(Ordering::Relaxed),
        "ui_ready": stats.ui_ready.load(Ordering::Relaxed),
        "ingress_connections": stats.ingress_connections.load(Ordering::Relaxed),
        "ui_clients": stats.ui_clients.load(Ordering::Relaxed),
        "samples_rx": stats.samples_rx.load(Ordering::Relaxed),
        "ui_drop": stats.ui_drop.load(Ordering::Relaxed),
        "db_drop": stats.db_drop.load(Ordering::Relaxed),
        "db_write_fail": stats.db_write_fail.load(Ordering::Relaxed),
        "last_db_error": last_db_error,
    })
    .to_string()
}

async fn run_health_server(addr: String, stats: Arc<CollectorStats>) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        let stats = stats.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = match socket.read(&mut buf).await {
                Ok(n) => n,
                Err(_) => return,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let first = req.lines().next().unwrap_or("");

            let (code, body) = if first.starts_with("GET /ready") {
                let ready = stats.ingress_ready.load(Ordering::Relaxed)
                    && stats.ui_ready.load(Ordering::Relaxed);
                if ready {
                    ("200 OK", "{\"ready\":true}".to_string())
                } else {
                    ("503 Service Unavailable", "{\"ready\":false}".to_string())
                }
            } else {
                ("200 OK", health_json(&stats))
            };

            let resp = format!(
                "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                code,
                body.len(),
                body
            );
            let _ = socket.write_all(resp.as_bytes()).await;
        });
    }
}

pub async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cfg = load_config();
    info!(?cfg, "collector config loaded");

    let stats = Arc::new(CollectorStats::default());
    let raw_bus = EventBus::new(cfg.bus_capacity);
    let processed_bus = EventBus::new(cfg.bus_capacity);
    let store = Store::default();
    let sessions: Arc<tokio::sync::RwLock<HashMap<String, DeviceSessionHandle>>> =
        Arc::new(tokio::sync::RwLock::new(HashMap::new()));
    let filter_config = PersistenceFilterConfig {
        enabled: cfg.db_filter_enabled,
        order: cfg.db_filter_order,
        sample_rate_hz: cfg.db_filter_sample_rate_hz,
        cutoff_hz: cfg.db_filter_cutoff_hz,
    };
    tokio::spawn(run_filtered_event_forwarder(
        raw_bus.subscribe(),
        processed_bus.clone(),
        filter_config,
    ));

    let disable_db = std::env::var("DEMO2_DISABLE_DB")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let (ui_tx, _) = broadcast::channel::<String>(cfg.ui_feed_capacity);
    tokio::spawn(run_ui_forwarder(
        processed_bus.subscribe(),
        ui_tx.clone(),
        stats.clone(),
    ));
    tokio::spawn(run_alarm_forwarder(
        processed_bus.subscribe(),
        processed_bus.clone(),
        stats.clone(),
    ));

    if disable_db {
        warn!("DEMO2_DISABLE_DB is enabled, persistence is disabled");
    } else {
        match start_pg_writer(&cfg, stats.clone()).await {
            Ok(db_tx) => {
                tokio::spawn(run_persistence_forwarder(
                    processed_bus.subscribe(),
                    db_tx,
                    stats.clone(),
                ));
                info!("postgres persistence enabled");
            }
            Err(err) => {
                warn!(error = %err, "postgres unavailable, running without persistence");
            }
        }
    }

    tokio::spawn(run_health_server(cfg.health_addr.clone(), stats.clone()));

    let can_enabled = env_flag("DEMO2_COLLECTOR_CAN_ENABLED").unwrap_or(cfg.can_enabled);
    if can_enabled {
        let can_channel = std::env::var("DEMO2_COLLECTOR_CAN_CHANNEL")
            .ok()
            .and_then(|v| v.parse::<u8>().ok())
            .unwrap_or(cfg.can_channel);
        let can_config = CanTransportConfig {
            tsmaster_bin: std::env::var("DEMO2_COLLECTOR_CAN_TSMASTER_BIN")
                .ok()
                .or_else(|| cfg.can_tsmaster_bin.clone())
                .map(Into::into),
            autostart_tsmaster: env_flag("DEMO2_COLLECTOR_CAN_AUTOSTART_TSMASTER")
                .unwrap_or(cfg.can_autostart_tsmaster),
            hardware_name: std::env::var("DEMO2_COLLECTOR_CAN_HW_NAME")
                .unwrap_or_else(|_| cfg.can_hardware_name.clone()),
            channel_index: can_channel,
            channel_count: i32::from(can_channel) + 1,
            arbitration_baud_kbps: std::env::var("DEMO2_COLLECTOR_CAN_BAUD_KBPS")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(cfg.can_baud_kbps),
            data_baud_kbps: std::env::var("DEMO2_COLLECTOR_CAN_DATA_BAUD_KBPS")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(cfg.can_data_baud_kbps),
            ..CanTransportConfig::default()
        };
        let sent_filter_config = SentFilterConfig {
            enabled: env_flag("DEMO2_SENT_FILTER_ENABLED").unwrap_or(cfg.sent_filter_enabled),
            window_size: std::env::var("DEMO2_SENT_FILTER_WINDOW")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(cfg.sent_filter_window)
                .max(1),
        };
        let bus_for_can = raw_bus.clone();
        tokio::spawn(async move {
            run_can_ingress(can_config, sent_filter_config, bus_for_can).await;
        });
    }

    if let Some(port_name) = cfg
        .serial_port
        .clone()
        .or_else(|| std::env::var("DEMO2_COLLECTOR_SERIAL_PORT").ok())
    {
        let serial_baud = std::env::var("DEMO2_COLLECTOR_SERIAL_BAUD")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(cfg.serial_baud);
        let serial_mode_text = std::env::var("DEMO2_COLLECTOR_SERIAL_MODE")
            .unwrap_or_else(|_| cfg.serial_mode.clone());
        let serial_mode = parse_serial_mode(&serial_mode_text).unwrap_or(SerialIngressMode::Sent);
        match serial_mode {
            SerialIngressMode::Legacy => {
                let device_id = format!("serial://{}", port_name);
                let handle = DeviceSession::spawn(
                    device_id.clone(),
                    Box::new(SerialTransport::new(port_name, serial_baud)),
                    Arc::new(SimpleFrameCodec::new(cfg.max_payload)),
                    raw_bus.clone(),
                    store.clone(),
                    SessionConfig {
                        enable_heartbeat: false,
                        reconnect_enabled: false,
                        ..SessionConfig::default()
                    },
                );
                sessions.write().await.insert(device_id, handle);
                publish_status(&raw_bus, "collector serial legacy session started");
            }
            _ => {
                let bus_for_serial = raw_bus.clone();
                tokio::spawn(async move {
                    run_serial_ingress(port_name, serial_baud, serial_mode, bus_for_serial).await;
                });
            }
        }
    }

    let ui_listener = TcpListener::bind(&cfg.ui_feed_addr).await?;
    stats.ui_ready.store(true, Ordering::Relaxed);
    let stats_for_ui = stats.clone();
    tokio::spawn(async move {
        loop {
            if let Ok((socket, _)) = ui_listener.accept().await {
                let rx = ui_tx.subscribe();
                tokio::spawn(serve_ui_client(socket, rx, stats_for_ui.clone()));
            }
        }
    });

    stats.ingress_ready.store(true, Ordering::Relaxed);

    println!("collector ingress listening on {}", cfg.ingress_addr);
    println!("collector ui feed listening on {}", cfg.ui_feed_addr);
    println!("collector health listening on {}", cfg.health_addr);

    run_tcp_ingress(
        &cfg.ingress_addr,
        cfg.max_payload,
        raw_bus.clone(),
        store.clone(),
        sessions.clone(),
    )
    .await?;

    Ok(())
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn estimate_amplitude(samples: &[f64], sample_rate_hz: f64, freq_hz: f64) -> f64 {
        let n = samples.len() as f64;
        let mut sin_acc = 0.0;
        let mut cos_acc = 0.0;

        for (idx, sample) in samples.iter().enumerate() {
            let phase = 2.0 * std::f64::consts::PI * freq_hz * idx as f64 / sample_rate_hz;
            sin_acc += sample * phase.sin();
            cos_acc += sample * phase.cos();
        }

        2.0 * (sin_acc.hypot(cos_acc)) / n
    }

    #[test]
    fn butterworth_lowpass_keeps_low_freq_and_reduces_high_freq() {
        let sample_rate_hz = 48_000.0;
        let low_freq_hz = 1_000.0;
        let high_freq_hz = 10_000.0;
        let mut filter = ButterworthCascade::new(10, sample_rate_hz as f32, 4_000.0).unwrap();

        let total_samples = 12_000usize;
        let mut output = Vec::with_capacity(total_samples);

        for idx in 0..total_samples {
            let t = idx as f64 / sample_rate_hz;
            let input = (2.0 * std::f64::consts::PI * low_freq_hz * t).sin()
                + (2.0 * std::f64::consts::PI * high_freq_hz * t).sin();
            output.push(filter.run(input));
        }

        // Ignore startup transient and only inspect the steady-state tail.
        let steady_state = &output[4_000..];
        let low_amp = estimate_amplitude(steady_state, sample_rate_hz, low_freq_hz);
        let high_amp = estimate_amplitude(steady_state, sample_rate_hz, high_freq_hz);

        assert!(
            low_amp > 0.7,
            "expected low-frequency component to remain visible, got amplitude {low_amp}"
        );
        assert!(
            high_amp < 0.1,
            "expected high-frequency component to be attenuated, got amplitude {high_amp}"
        );
        assert!(
            low_amp > high_amp * 8.0,
            "expected low-frequency component to dominate after filtering, low={low_amp}, high={high_amp}"
        );
    }

    #[test]
    fn butterworth_lowpass_accepts_manual_input_samples() {
        let mut filter = ButterworthCascade::new(10, 48_000.0, 4_000.0).unwrap();
        let mut input = Vec::new();
        input.extend(std::iter::repeat_n(0.0, 80));
        input.extend(std::iter::repeat_n(100.0, 160));
        input.extend(std::iter::repeat_n(0.0, 160));

        let output = input
            .iter()
            .map(|sample| filter.run(*sample))
            .collect::<Vec<_>>();

        assert_eq!(output.len(), input.len());

        println!(
            "manual input  (samples 70..110) = {:?}",
            &input[70..110]
        );
        println!(
            "filtered out  (samples 70..110) = {:?}",
            output[70..110]
                .iter()
                .map(|v| format!("{v:.6}"))
                .collect::<Vec<_>>()
        );
        println!(
            "filtered out  (samples 180..220) = {:?}",
            output[180..220]
                .iter()
                .map(|v| format!("{v:.6}"))
                .collect::<Vec<_>>()
        );
        println!(
            "filtered out  (samples 230..270) = {:?}",
            output[230..270]
                .iter()
                .map(|v| format!("{v:.6}"))
                .collect::<Vec<_>>()
        );
    }
}
