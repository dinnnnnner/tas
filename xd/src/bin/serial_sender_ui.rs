use anyhow::{Context, bail};
use bytes::BytesMut;
use demo2::protocol::demo_serial::{CMD_STREAM, GROUP_COUNT, GROUP_SIZE, HEADER, STREAM_BODY_LEN};
use demo2::protocol::{Frame, FrameCodec, SentFrameCodec, SimpleFrameCodec};
use demo2::transport::can::{CanTransport, CanTransportConfig, CanTxFrame};
use eframe::egui;
use serialport::FlowControl;
use std::collections::VecDeque;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::time::{self, MissedTickBehavior};

const DEFAULT_PORT: &str = "COM4";
const DEFAULT_BAUD: &str = "2000000";
const DEFAULT_SENSOR_COUNT: &str = "4";
const DEFAULT_INTERVAL_MS: &str = "30";
const DEFAULT_DURATION_SECS: &str = "60";
const DEFAULT_KIND: u8 = 0x34;
const DEFAULT_MAX_PAYLOAD: usize = 4096;
const MAX_LOG_LINES: usize = 300;
const DEMO_GROUP_RATE_HZ: f64 = 6250.0;
const DEFAULT_CAN_TX_ID: u32 = 0x123;
const DEFAULT_CAN_TX_DLC: u8 = 8;
const DEFAULT_CAN_TX_DATA: [u8; 8] = [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
const DEFAULT_CAN_EXPORT_PATH: &str =
    "C:\\Users\\admin\\Desktop\\xd\\target\\debug\\exports\\can_export_04-22_13-18-27_04-22_13-13-25_04-22_13-18-25.txt";
const CAN_AXIS_X_ID: u32 = 0x100;
const CAN_AXIS_Y_ID: u32 = 0x102;
const CAN_AXIS_Z_ID: u32 = 0x104;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputFormat {
    Demo,
    Legacy,
    Sent1,
    Sent2,
    Sent3,
}

impl OutputFormat {
    fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "demo" => Some(Self::Demo),
            "legacy" => Some(Self::Legacy),
            "sent1" => Some(Self::Sent1),
            "sent2" => Some(Self::Sent2),
            "sent3" | "sent539" => Some(Self::Sent3),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Demo => "DEMO",
            Self::Legacy => "legacy",
            Self::Sent1 => "SENT1",
            Self::Sent2 => "SENT2",
            Self::Sent3 => "SENT3",
        }
    }

    fn frame_us(self) -> Option<u64> {
        match self {
            Self::Demo => Some(40_960),
            Self::Legacy => None,
            Self::Sent1 => Some(439),
            Self::Sent2 => Some(484),
            Self::Sent3 => Some(539),
        }
    }

    fn all() -> [Self; 5] {
        [Self::Demo, Self::Legacy, Self::Sent1, Self::Sent2, Self::Sent3]
    }

    fn collector_serial_mode(self) -> &'static str {
        match self {
            Self::Demo => "demo",
            Self::Legacy => "legacy",
            Self::Sent1 | Self::Sent2 | Self::Sent3 => "sent",
        }
    }
}

#[derive(Clone)]
struct SenderConfig {
    port: String,
    baud: u32,
    sensor_count: usize,
    interval_ms: u64,
    duration_secs: u64,
    format: OutputFormat,
}

enum WorkerMsg {
    Log(String),
    Stats {
        sent_frames: u64,
        ack_frames: u64,
        last_req: u64,
    },
    Finished {
        ok: bool,
        message: String,
    },
}

#[derive(Clone, Debug)]
struct CanExportRow {
    ts_ms: i64,
    identifier: u32,
    value: i32,
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
        fonts
            .font_data
            .insert("cn_font".to_owned(), egui::FontData::from_owned(bytes).into());
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

fn sensor_value(sensor_id: usize, t_sec: f64) -> f64 {
    let sid = sensor_id as f64;
    let amplitude = 420.0 + (sensor_id % 4) as f64 * 45.0;
    let phase = sid * 0.52;
    let slow_wave = ((2.0 * std::f64::consts::PI * t_sec / 3.0) + phase).sin() * amplitude;
    let ripple_freq = (0.05_f64).max((0.8 + sid * 0.08) / 80.0);
    let ripple =
        ((2.0 * std::f64::consts::PI * ripple_freq * t_sec) + phase * 2.0).sin() * amplitude * 0.08;
    let sensor_bias = (sensor_id as f64 - 4.5) * 18.0;
    slow_wave + ripple + sensor_bias
}

fn sent_channel_value(pair_id: usize, t_sec: f64, phase_shift_deg: f64, amplitude: f64) -> u16 {
    let pair_phase = pair_id as f64 * 0.37;
    let base = ((2.0 * std::f64::consts::PI * t_sec / 3.0)
        + pair_phase
        + phase_shift_deg.to_radians())
    .sin()
        * amplitude;
    let ripple = ((2.0 * std::f64::consts::PI * 1.6 * t_sec)
        + pair_phase * 1.7
        + phase_shift_deg.to_radians() * 0.3)
    .sin()
        * amplitude
        * 0.06;
    let centered = 2048.0 + base + ripple;
    centered.round().clamp(0.0, 4095.0) as u16
}

fn demo_axis_value(axis: usize, t_sec: f64) -> i16 {
    let phase = match axis {
        0 => 0.0,
        1 => 105.0_f64.to_radians(),
        _ => 225.0_f64.to_radians(),
    };
    let base = ((2.0 * std::f64::consts::PI * t_sec / 1.2) + phase).sin() * 1400.0;
    let ripple =
        ((2.0 * std::f64::consts::PI * 5.8 * t_sec) + phase * 0.6).sin() * 220.0;
    let fine =
        ((2.0 * std::f64::consts::PI * 12.0 * t_sec) + phase * 1.3).sin() * 90.0;
    let bias = match axis {
        0 => 120.0,
        1 => -80.0,
        _ => 45.0,
    };
    (base + ripple + fine + bias)
        .round()
        .clamp(-8192.0, 8191.0) as i16
}

fn encode_demo_axis_sample(value: i16) -> [u8; 2] {
    let raw = (i32::from(value) * 4).clamp(i16::MIN as i32, i16::MAX as i32) as i16;
    raw.to_le_bytes()
}

fn demo_alarm_active(t_sec: f64) -> bool {
    let phase = t_sec.rem_euclid(8.0);
    (2.0..3.2).contains(&phase) || (5.4..6.1).contains(&phase)
}

fn build_demo_frame(frame_index: u64, start: Instant) -> Vec<u8> {
    let mut raw = Vec::with_capacity(4 + STREAM_BODY_LEN);
    raw.push(HEADER);
    raw.extend_from_slice(&((4 + STREAM_BODY_LEN) as u16).to_le_bytes());
    raw.push(CMD_STREAM);

    for group_idx in 0..GROUP_COUNT {
        let t_sec = start.elapsed().as_secs_f64()
            + (frame_index as f64 * GROUP_COUNT as f64 + group_idx as f64) / DEMO_GROUP_RATE_HZ;
        let x = encode_demo_axis_sample(demo_axis_value(0, t_sec));
        let y = encode_demo_axis_sample(demo_axis_value(1, t_sec));
        let z = encode_demo_axis_sample(demo_axis_value(2, t_sec));
        let alarm = if demo_alarm_active(t_sec) { 0x01 } else { 0x00 };

        let mut group = [0u8; GROUP_SIZE];
        group[24..26].copy_from_slice(&x);
        group[26..28].copy_from_slice(&y);
        group[28..30].copy_from_slice(&z);
        group[30] = alarm;
        raw.extend_from_slice(&group);
    }

    raw
}

fn build_sent_frame(
    pair_id: usize,
    frame_index: u64,
    t_sec: f64,
    format: OutputFormat,
) -> demo2::protocol::SentFrame {
    let channel_1 = sent_channel_value(pair_id, t_sec, 0.0, 1750.0);
    let channel_2 = sent_channel_value(pair_id, t_sec, 110.0, 980.0);
    let status = (pair_id as u8) & 0x0F;
    match format {
        OutputFormat::Demo => SentFrameCodec::with_crc(status, channel_1, channel_2, 0x0),
        OutputFormat::Legacy => SentFrameCodec::with_crc(status, channel_1, channel_2, 0x0),
        OutputFormat::Sent1 => SentFrameCodec::with_crc(status, channel_1, channel_2, 0x1),
        OutputFormat::Sent2 => SentFrameCodec::with_crc(status, channel_1, channel_2, 0x6),
        OutputFormat::Sent3 => {
            let rolling_counter = ((frame_index + pair_id as u64) & 0x0F) as u8;
            SentFrameCodec::sent3_with_crc(status, channel_1, rolling_counter, 0x0B)
        }
    }
}

fn write_frame_all(port: &mut dyn serialport::SerialPort, frame: &[u8]) -> anyhow::Result<()> {
    let mut offset = 0;
    let mut retries = 0_u32;

    while offset < frame.len() {
        match port.write(&frame[offset..]) {
            Ok(0) => {
                retries = retries.saturating_add(1);
                if retries > 20 {
                    bail!("serial write made no progress");
                }
                thread::sleep(Duration::from_millis(20));
            }
            Ok(n) => {
                offset += n;
                retries = 0;
            }
            Err(err) if err.kind() == std::io::ErrorKind::TimedOut => {
                retries = retries.saturating_add(1);
                if retries > 20 {
                    return Err(err.into());
                }
                thread::sleep(Duration::from_millis(20));
            }
            Err(err) => return Err(err.into()),
        }
    }

    port.flush()?;
    Ok(())
}

fn drain_ack(
    port: &mut dyn serialport::SerialPort,
    codec: &SimpleFrameCodec,
    ack_buf: &mut BytesMut,
    chunk: &mut [u8],
    tx: &Sender<WorkerMsg>,
) -> anyhow::Result<u64> {
    let mut ack_count = 0_u64;

    loop {
        match port.read(chunk) {
            Ok(0) => break,
            Ok(n) => ack_buf.extend_from_slice(&chunk[..n]),
            Err(err) if err.kind() == std::io::ErrorKind::TimedOut => break,
            Err(err) => return Err(err.into()),
        }

        loop {
            match codec.try_decode(ack_buf) {
                Ok(Some(frame)) => {
                    ack_count = ack_count.saturating_add(1);
                    let payload = String::from_utf8_lossy(&frame.payload);
                    let _ = tx.send(WorkerMsg::Log(format!(
                        "ack request_id={} kind=0x{:02X} payload={}",
                        frame.request_id, frame.kind, payload
                    )));
                }
                Ok(None) => break,
                Err(err) => {
                    let _ = tx.send(WorkerMsg::Log(format!("ack decode error: {err}")));
                    break;
                }
            }
        }
    }

    Ok(ack_count)
}

fn run_sender(config: SenderConfig, stop_flag: Arc<AtomicBool>, tx: Sender<WorkerMsg>) -> anyhow::Result<()> {
    let mut port = serialport::new(&config.port, config.baud)
        .timeout(Duration::from_millis(200))
        .flow_control(FlowControl::None)
        .data_bits(serialport::DataBits::Eight)
        .parity(serialport::Parity::None)
        .stop_bits(serialport::StopBits::One)
        .open()
        .with_context(|| format!("open serial port failed: {} @ {}", config.port, config.baud))?;

    let _ = port.write_data_terminal_ready(true);
    let _ = port.write_request_to_send(true);
    thread::sleep(Duration::from_millis(100));

    match config.format {
        OutputFormat::Demo => {
            let rt = Builder::new_current_thread().enable_time().build()?;
            rt.block_on(run_demo_sender(config, stop_flag, tx, &mut *port))
        }
        OutputFormat::Legacy => run_legacy_sender(config, stop_flag, tx, &mut *port),
        _ => {
            let rt = Builder::new_current_thread().enable_time().build()?;
            rt.block_on(run_sent_sender(config, stop_flag, tx, &mut *port))
        }
    }
}

fn run_legacy_sender(
    config: SenderConfig,
    stop_flag: Arc<AtomicBool>,
    tx: Sender<WorkerMsg>,
    port: &mut dyn serialport::SerialPort,
) -> anyhow::Result<()> {
    let codec = SimpleFrameCodec::new(DEFAULT_MAX_PAYLOAD);
    let total_ticks = (config.duration_secs * 1000) / config.interval_ms;
    let start = Instant::now();
    let mut req_id = 10_000_u64;
    let mut sent_frames = 0_u64;
    let mut ack_frames = 0_u64;
    let mut ack_buf = BytesMut::with_capacity(1024);
    let mut ack_chunk = [0u8; 256];

    let _ = tx.send(WorkerMsg::Log(format!(
        "serial sender start: mode=legacy, port={}, baud={}, sensors={}, interval={}ms, duration={}s",
        config.port, config.baud, config.sensor_count, config.interval_ms, config.duration_secs
    )));

    for tick in 0..total_ticks {
        if stop_flag.load(Ordering::Relaxed) {
            let _ = tx.send(WorkerMsg::Finished { ok: true, message: "send stopped by user".to_string() });
            return Ok(());
        }
        for sensor_id in 0..config.sensor_count {
            if stop_flag.load(Ordering::Relaxed) {
                let _ = tx.send(WorkerMsg::Finished { ok: true, message: "send stopped by user".to_string() });
                return Ok(());
            }
            let t_sec = start.elapsed().as_secs_f64();
            let value = sensor_value(sensor_id, t_sec);
            let payload = format!("sid={sensor_id},value={value:.3}").into_bytes();
            let frame = Frame { request_id: req_id, kind: DEFAULT_KIND, payload };
            req_id = req_id.saturating_add(1);

            let mut out = BytesMut::new();
            codec.encode(&frame, &mut out)?;
            write_frame_all(port, &out).with_context(|| {
                format!("serial write failed on {} for req_id={}", config.port, frame.request_id)
            })?;
            sent_frames = sent_frames.saturating_add(1);

            if tick % 10 == 0 {
                let _ = tx.send(WorkerMsg::Log(format!(
                    "tick={} sensor={} t={:.1}s value={:.3} req_id={}",
                    tick, sensor_id, t_sec, value, frame.request_id
                )));
            }

            ack_frames = ack_frames.saturating_add(drain_ack(port, &codec, &mut ack_buf, &mut ack_chunk, &tx)?);
            let _ = tx.send(WorkerMsg::Stats { sent_frames, ack_frames, last_req: frame.request_id });
        }
        thread::sleep(Duration::from_millis(config.interval_ms));
    }

    let _ = tx.send(WorkerMsg::Finished {
        ok: true,
        message: format!("done in {:.2}s, sent_frames={}, ack_frames={}", start.elapsed().as_secs_f64(), sent_frames, ack_frames),
    });
    Ok(())
}

fn parse_can_export_file(path: &str) -> anyhow::Result<Vec<CanExportRow>> {
    let text = fs::read_to_string(path).with_context(|| format!("read CAN export failed: {path}"))?;
    let mut rows = Vec::new();

    for (line_no, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with("ts_ms\t") {
            continue;
        }

        let cols = line.split('\t').collect::<Vec<_>>();
        if cols.len() < 5 {
            bail!("invalid CAN export row at line {}: {}", line_no + 1, line);
        }

        let ts_ms = cols[0]
            .parse::<i64>()
            .with_context(|| format!("invalid ts_ms at line {}", line_no + 1))?;
        let identifier = match cols[2].trim() {
            "x" => CAN_AXIS_X_ID,
            "y" => CAN_AXIS_Y_ID,
            "z" => CAN_AXIS_Z_ID,
            axis => bail!("invalid axis '{axis}' at line {}", line_no + 1),
        };
        let value = cols[3]
            .parse::<f64>()
            .with_context(|| format!("invalid value at line {}", line_no + 1))?
            .round() as i32;

        rows.push(CanExportRow {
            ts_ms,
            identifier,
            value,
        });
    }

    rows.sort_by_key(|row| row.ts_ms);
    Ok(rows)
}

async fn run_demo_sender(
    config: SenderConfig,
    stop_flag: Arc<AtomicBool>,
    tx: Sender<WorkerMsg>,
    port: &mut dyn serialport::SerialPort,
) -> anyhow::Result<()> {
    let frame_us = config
        .format
        .frame_us()
        .context("DEMO mode requires fixed frame length")?;
    let start = Instant::now();
    let deadline = start + Duration::from_secs(config.duration_secs);
    let mut ticker = time::interval(Duration::from_micros(frame_us));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut sent_frames = 0_u64;

    let _ = tx.send(WorkerMsg::Log(format!(
        "serial sender start: mode=DEMO, port={}, baud={}, axes=3, groups/frame={}, t_frame={}us, duration={}s",
        config.port, config.baud, GROUP_COUNT, frame_us, config.duration_secs
    )));

    while Instant::now() < deadline {
        if stop_flag.load(Ordering::Relaxed) {
            let _ = tx.send(WorkerMsg::Finished {
                ok: true,
                message: "send stopped by user".to_string(),
            });
            return Ok(());
        }

        ticker.tick().await;
        let raw = build_demo_frame(sent_frames, start);
        write_frame_all(port, &raw).with_context(|| {
            format!(
                "serial DEMO write failed on {} for frame={}",
                config.port, sent_frames
            )
        })?;
        sent_frames = sent_frames.saturating_add(1);

        if sent_frames <= 3 || sent_frames % 20 == 0 {
            let t_sec = start.elapsed().as_secs_f64();
            let _ = tx.send(WorkerMsg::Log(format!(
                "DEMO frame={} xyz=({},{},{}) alarm={} bytes={}",
                sent_frames,
                demo_axis_value(0, t_sec),
                demo_axis_value(1, t_sec),
                demo_axis_value(2, t_sec),
                demo_alarm_active(t_sec),
                raw.len()
            )));
        }
        let _ = tx.send(WorkerMsg::Stats {
            sent_frames,
            ack_frames: 0,
            last_req: sent_frames,
        });
    }

    let _ = tx.send(WorkerMsg::Finished {
        ok: true,
        message: format!(
            "done in {:.2}s, sent_frames={}, mode=DEMO",
            start.elapsed().as_secs_f64(),
            sent_frames
        ),
    });
    Ok(())
}

async fn run_sent_sender(
    config: SenderConfig,
    stop_flag: Arc<AtomicBool>,
    tx: Sender<WorkerMsg>,
    port: &mut dyn serialport::SerialPort,
) -> anyhow::Result<()> {
    let frame_us = config.format.frame_us().context("SENT mode requires fixed frame length")?;
    let start = Instant::now();
    let deadline = start + Duration::from_secs(config.duration_secs);
    let mut ticker = time::interval(Duration::from_micros(frame_us));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut sent_frames = 0_u64;

    let _ = tx.send(WorkerMsg::Log(format!(
        "serial sender start: mode={}, port={}, baud={}, pairs={}, t_frame={}us, duration={}s",
        config.format.label(),
        config.port,
        config.baud,
        config.sensor_count,
        frame_us,
        config.duration_secs
    )));

    while Instant::now() < deadline {
        if stop_flag.load(Ordering::Relaxed) {
            let _ = tx.send(WorkerMsg::Finished { ok: true, message: "send stopped by user".to_string() });
            return Ok(());
        }

        ticker.tick().await;
        let t_sec = start.elapsed().as_secs_f64();
        for pair_id in 0..config.sensor_count {
            let sent = build_sent_frame(pair_id, sent_frames, t_sec, config.format);
            let raw = SentFrameCodec::encode_frame(sent);
            write_frame_all(port, &raw).with_context(|| {
                format!("serial SENT write failed on {} for frame={} pair={}", config.port, sent_frames, pair_id)
            })?;
            sent_frames = sent_frames.saturating_add(1);

            if sent_frames <= 5 || sent_frames % 200 == 0 {
                let line = if config.format == OutputFormat::Sent3 {
                    let rolling_counter = (sent.channel_2 & 0x0F) as u8;
                    let inverted_copy = ((sent.channel_2 >> 4) & 0x0F) as u8;
                    let check_nibble = ((sent.channel_2 >> 8) & 0x0F) as u8;
                    format!(
                        "{} frame={} pair={} status=0x{:X} angle=0x{:03X} rc=0x{:X} inv=0x{:X} chk=0x{:X} crc=0x{:X} pause=0x{:X} raw={:02X?}",
                        config.format.label(),
                        sent_frames,
                        pair_id,
                        sent.status,
                        sent.channel_1,
                        rolling_counter,
                        inverted_copy,
                        check_nibble,
                        sent.crc,
                        sent.pause,
                        raw
                    )
                } else {
                    format!(
                        "{} frame={} pair={} status=0x{:X} p1=0x{:03X} p2=0x{:03X} crc=0x{:X} pause=0x{:X} raw={:02X?}",
                        config.format.label(),
                        sent_frames,
                        pair_id,
                        sent.status,
                        sent.channel_1,
                        sent.channel_2,
                        sent.crc,
                        sent.pause,
                        raw
                    )
                };
                let _ = tx.send(WorkerMsg::Log(line));
            }
            let _ = tx.send(WorkerMsg::Stats { sent_frames, ack_frames: 0, last_req: sent_frames });
        }
    }

    let _ = tx.send(WorkerMsg::Finished {
        ok: true,
        message: format!("done in {:.2}s, sent_frames={}, mode={}", start.elapsed().as_secs_f64(), sent_frames, config.format.label()),
    });
    Ok(())
}

struct SerialSenderUiApp {
    port: String,
    baud: String,
    format: String,
    sensor_count: String,
    interval_ms: String,
    duration_secs: String,
    status: String,
    logs: VecDeque<String>,
    sent_frames: u64,
    ack_frames: u64,
    last_req: u64,
    can_tx_counter: u8,
    can_export_path: String,
    worker_rx: Option<Receiver<WorkerMsg>>,
    stop_flag: Option<Arc<AtomicBool>>,
}

impl SerialSenderUiApp {
    fn new() -> Self {
        Self {
            port: DEFAULT_PORT.to_string(),
            baud: DEFAULT_BAUD.to_string(),
            format: "sent1".to_string(),
            sensor_count: DEFAULT_SENSOR_COUNT.to_string(),
            interval_ms: DEFAULT_INTERVAL_MS.to_string(),
            duration_secs: DEFAULT_DURATION_SECS.to_string(),
            status: "idle".to_string(),
            logs: VecDeque::with_capacity(MAX_LOG_LINES),
            sent_frames: 0,
            ack_frames: 0,
            last_req: 0,
            can_tx_counter: 0,
            can_export_path: DEFAULT_CAN_EXPORT_PATH.to_string(),
            worker_rx: None,
            stop_flag: None,
        }
    }

    fn push_log(&mut self, line: impl Into<String>) {
        if self.logs.len() >= MAX_LOG_LINES {
            let _ = self.logs.pop_front();
        }
        self.logs.push_back(line.into());
    }

    fn is_running(&self) -> bool {
        self.stop_flag.is_some()
    }

    fn parse_config(&self) -> anyhow::Result<SenderConfig> {
        let port = self.port.trim().to_string();
        if port.is_empty() {
            bail!("serial port cannot be empty");
        }

        let baud = self.baud.trim().parse::<u32>().context("invalid baud")?;
        let sensor_count = self
            .sensor_count
            .trim()
            .parse::<usize>()
            .context("invalid sensors")?;
        let format = OutputFormat::parse(self.format.trim()).context("invalid format")?;
        let interval_ms = self
            .interval_ms
            .trim()
            .parse::<u64>()
            .context("invalid interval")?;
        let duration_secs = self
            .duration_secs
            .trim()
            .parse::<u64>()
            .context("invalid duration")?;

        if baud == 0 || sensor_count == 0 {
            bail!("baud and sensors must be > 0");
        }
        if format == OutputFormat::Legacy && interval_ms == 0 {
            bail!("interval must be > 0 in legacy mode");
        }

        Ok(SenderConfig {
            port,
            baud,
            format,
            sensor_count,
            interval_ms,
            duration_secs,
        })
    }

    fn sync_collector_mode(format: OutputFormat) -> anyhow::Result<String> {
        let path = "config.toml";
        let mut root = match fs::read_to_string(path) {
            Ok(text) => text
                .parse::<toml::Value>()
                .context("parse config.toml failed")?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                toml::Value::Table(toml::map::Map::new())
            }
            Err(err) => return Err(err).with_context(|| format!("read {path} failed")),
        };

        let Some(root_table) = root.as_table_mut() else {
            bail!("config.toml root must be a table");
        };
        let collector = root_table
            .entry("collector".to_string())
            .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
        let Some(collector_table) = collector.as_table_mut() else {
            bail!("config.toml [collector] must be a table");
        };

        let mode = format.collector_serial_mode();
        let already_same = collector_table
            .get("serial_mode")
            .and_then(toml::Value::as_str)
            == Some(mode);
        collector_table.insert("serial_mode".to_string(), toml::Value::String(mode.to_string()));

        let rendered = toml::to_string_pretty(&root).context("serialize config.toml failed")?;
        fs::write(path, rendered).with_context(|| format!("write {path} failed"))?;

        if already_same {
            Ok(format!("collector.serial_mode already '{}'", mode))
        } else {
            Ok(format!("updated collector.serial_mode -> '{}'", mode))
        }
    }

    fn sync_can_collector_config() -> anyhow::Result<String> {
        let path = "config.toml";
        let mut root = match fs::read_to_string(path) {
            Ok(text) => text
                .parse::<toml::Value>()
                .context("parse config.toml failed")?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                toml::Value::Table(toml::map::Map::new())
            }
            Err(err) => return Err(err).with_context(|| format!("read {path} failed")),
        };

        let Some(root_table) = root.as_table_mut() else {
            bail!("config.toml root must be a table");
        };
        let collector = root_table
            .entry("collector".to_string())
            .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
        let Some(collector_table) = collector.as_table_mut() else {
            bail!("config.toml [collector] must be a table");
        };

        let already_enabled = collector_table
            .get("can_enabled")
            .and_then(toml::Value::as_bool)
            == Some(true);
        collector_table.insert("can_enabled".to_string(), toml::Value::Boolean(true));
        collector_table.insert(
            "can_hardware_name".to_string(),
            toml::Value::String("TC1012".to_string()),
        );
        collector_table.insert("can_channel".to_string(), toml::Value::Integer(0));
        collector_table.insert("can_baud_kbps".to_string(), toml::Value::Integer(500));
        collector_table.insert("can_data_baud_kbps".to_string(), toml::Value::Integer(2000));

        let rendered = toml::to_string_pretty(&root).context("serialize config.toml failed")?;
        fs::write(path, rendered).with_context(|| format!("write {path} failed"))?;

        if already_enabled {
            Ok("collector.can_enabled already true".to_string())
        } else {
            Ok("enabled collector CAN ingress (TC1012 ch0, 500/2000 kbps)".to_string())
        }
    }

    fn start_worker(&mut self) {
        let config = match self.parse_config() {
            Ok(cfg) => cfg,
            Err(err) => {
                self.status = format!("config error: {err}");
                self.push_log(self.status.clone());
                return;
            }
        };

        match Self::sync_collector_mode(config.format) {
            Ok(message) => self.push_log(message),
            Err(err) => {
                self.status = format!("config sync error: {err}");
                self.push_log(self.status.clone());
                return;
            }
        }

        let (tx, rx) = mpsc::channel();
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag2 = stop_flag.clone();
        self.worker_rx = Some(rx);
        self.stop_flag = Some(stop_flag);
        self.status = format!("starting: {} @ {}", config.port, config.baud);
        self.push_log(self.status.clone());
        self.push_log(format!(
            "collector mode synced to '{}'; restart collector_service if it is already running",
            config.format.collector_serial_mode()
        ));
        self.sent_frames = 0;
        self.ack_frames = 0;
        self.last_req = 0;

        thread::spawn(move || {
            if let Err(err) = run_sender(config, stop_flag2, tx.clone()) {
                let _ = tx.send(WorkerMsg::Finished {
                    ok: false,
                    message: format!("{err:#}"),
                });
            }
        });
    }

    fn send_can_frame_once(&mut self) {
        if self.worker_rx.is_some() {
            self.status = "background task still running".to_string();
            self.push_log(self.status.clone());
            return;
        }

        let counter = self.can_tx_counter;
        self.can_tx_counter = self.can_tx_counter.wrapping_add(1);
        let (tx, rx) = mpsc::channel();
        self.worker_rx = Some(rx);
        self.status = format!("sending CAN frame counter=0x{counter:02X}");
        self.push_log(self.status.clone());

        thread::spawn(move || {
            let result = (|| -> anyhow::Result<String> {
                let rt = Builder::new_current_thread().enable_all().build()?;
                rt.block_on(async move {
                    let mut transport = CanTransport::new(CanTransportConfig::default());
                    transport
                        .connect()
                        .await
                        .context("connect CAN transport failed")?;

                    let mut data = DEFAULT_CAN_TX_DATA;
                    data[0] = counter;
                    let frame = CanTxFrame::new(0, DEFAULT_CAN_TX_ID, DEFAULT_CAN_TX_DLC, data);
                    transport
                        .transmit(frame)
                        .await
                        .context("transmit CAN frame failed")?;
                    transport.close().await.ok();

                    Ok(format!(
                        "CAN TX sent: ch=0 id=0x{:X} dlc={} data={:02X?}",
                        DEFAULT_CAN_TX_ID, DEFAULT_CAN_TX_DLC, data
                    ))
                })
            })();

            match result {
                Ok(message) => {
                    let _ = tx.send(WorkerMsg::Log(message.clone()));
                    let _ = tx.send(WorkerMsg::Finished { ok: true, message });
                }
                Err(err) => {
                    let _ = tx.send(WorkerMsg::Finished {
                        ok: false,
                        message: format!("{err:#}"),
                    });
                }
            }
        });
    }

    fn start_can_export_replay(&mut self) {
        if self.worker_rx.is_some() {
            self.status = "background task still running".to_string();
            self.push_log(self.status.clone());
            return;
        }

        let export_path = self.can_export_path.trim().to_string();
        if export_path.is_empty() {
            self.status = "CAN export path cannot be empty".to_string();
            self.push_log(self.status.clone());
            return;
        }

        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag2 = stop_flag.clone();
        let (tx, rx) = mpsc::channel();
        let tx_for_worker = tx.clone();
        self.worker_rx = Some(rx);
        self.stop_flag = Some(stop_flag);
        self.sent_frames = 0;
        self.ack_frames = 0;
        self.last_req = 0;
        self.status = format!("replaying CAN export: {export_path}");
        self.push_log(self.status.clone());

        thread::spawn(move || {
            let result = (|| -> anyhow::Result<()> {
                let rows = parse_can_export_file(&export_path)?;
                if rows.is_empty() {
                    bail!("CAN export file contains no replay rows");
                }

                let rt = Builder::new_current_thread().enable_all().build()?;
                rt.block_on(async move {
                    let mut transport = CanTransport::new(CanTransportConfig::default());
                    transport
                        .connect()
                        .await
                        .context("connect CAN transport failed")?;

                    let _ = tx_for_worker.send(WorkerMsg::Log(format!(
                        "CAN export replay start: path={}, rows={}",
                        export_path,
                        rows.len()
                    )));

                    let mut sent_frames = 0_u64;
                    let mut last_ts_ms = rows[0].ts_ms;
                    for (idx, row) in rows.into_iter().enumerate() {
                        if stop_flag2.load(Ordering::Relaxed) {
                            let _ = tx_for_worker.send(WorkerMsg::Finished {
                                ok: true,
                                message: "CAN export replay stopped by user".to_string(),
                            });
                            transport.close().await.ok();
                            return Ok(());
                        }

                        let delay_ms = (row.ts_ms - last_ts_ms).max(0) as u64;
                        if delay_ms > 0 {
                            time::sleep(Duration::from_millis(delay_ms)).await;
                        }
                        last_ts_ms = row.ts_ms;

                        let mut data = [0u8; 8];
                        data[..4].copy_from_slice(&row.value.to_be_bytes());
                        let frame = CanTxFrame::new(0, row.identifier, 8, data);
                        transport
                            .transmit(frame)
                            .await
                            .with_context(|| format!("transmit CAN export row failed at index {idx}"))?;

                        sent_frames = sent_frames.saturating_add(1);
                        if sent_frames <= 5 || sent_frames % 200 == 0 {
                            let axis = match row.identifier {
                                CAN_AXIS_X_ID => "x",
                                CAN_AXIS_Y_ID => "y",
                                CAN_AXIS_Z_ID => "z",
                                _ => "?",
                            };
                            let _ = tx_for_worker.send(WorkerMsg::Log(format!(
                                "CAN export sent row={} axis={} ts_ms={} value={} id=0x{:X}",
                                sent_frames, axis, row.ts_ms, row.value, row.identifier
                            )));
                        }
                        let _ = tx_for_worker.send(WorkerMsg::Stats {
                            sent_frames,
                            ack_frames: 0,
                            last_req: sent_frames,
                        });
                    }

                    transport.close().await.ok();
                    let _ = tx_for_worker.send(WorkerMsg::Finished {
                        ok: true,
                        message: format!("CAN export replay done, sent_frames={sent_frames}"),
                    });
                    Ok(())
                })
            })();

            if let Err(err) = result {
                let _ = tx.send(WorkerMsg::Finished {
                    ok: false,
                    message: format!("{err:#}"),
                });
            }
        });
    }

    fn stop_worker(&mut self) {
        if let Some(flag) = &self.stop_flag {
            flag.store(true, Ordering::Relaxed);
            self.status = "stop requested".to_string();
            self.push_log("stop requested");
        }
    }

    fn drain_worker_messages(&mut self) {
        let mut clear_worker = false;
        loop {
            let msg = match self.worker_rx.as_ref() {
                Some(rx) => match rx.try_recv() {
                    Ok(msg) => msg,
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        clear_worker = true;
                        break;
                    }
                },
                None => break,
            };

            match msg {
                WorkerMsg::Log(line) => self.push_log(line),
                WorkerMsg::Stats {
                    sent_frames,
                    ack_frames,
                    last_req,
                } => {
                    self.sent_frames = sent_frames;
                    self.ack_frames = ack_frames;
                    self.last_req = last_req;
                }
                WorkerMsg::Finished { ok, message } => {
                    self.status = if ok {
                        format!("finished: {message}")
                    } else {
                        format!("failed: {message}")
                    };
                    self.push_log(self.status.clone());
                    clear_worker = true;
                }
            }
        }

        if clear_worker {
            self.worker_rx = None;
            self.stop_flag = None;
        }
    }
}

impl eframe::App for SerialSenderUiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_worker_messages();

        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.heading("Serial Sender UI");
            ui.horizontal(|ui| {
                ui.label("Port");
                ui.text_edit_singleline(&mut self.port);
                ui.label("Baud");
                ui.text_edit_singleline(&mut self.baud);
                ui.label("Format");
                egui::ComboBox::from_id_salt("serial_format")
                    .selected_text(&self.format)
                    .show_ui(ui, |ui| {
                        for format in OutputFormat::all() {
                            ui.selectable_value(&mut self.format, format.label().to_ascii_lowercase(), format.label());
                        }
                    });
                ui.label("Sensors");
                ui.text_edit_singleline(&mut self.sensor_count);
            });
            ui.horizontal(|ui| {
                ui.label("Quick mode");
                for format in [OutputFormat::Demo, OutputFormat::Sent1, OutputFormat::Sent2, OutputFormat::Sent3] {
                    let selected = self.format.eq_ignore_ascii_case(format.label());
                    if ui.selectable_label(selected, format.label()).clicked() {
                        self.format = format.label().to_ascii_lowercase();
                    }
                }
                if ui.button("Legacy").clicked() {
                    self.format = "legacy".to_string();
                }
                if ui.button("CAN").clicked() {
                    match Self::sync_can_collector_config() {
                        Ok(message) => {
                            self.status = "collector CAN config synced".to_string();
                            self.push_log(message);
                            self.push_log(
                                "collector will start CAN listen after restart; frame parsing stays for the next step",
                            );
                        }
                        Err(err) => {
                            self.status = format!("CAN config sync error: {err}");
                            self.push_log(self.status.clone());
                        }
                    }
                }
                if ui.button("Send CAN Frame").clicked() {
                    self.send_can_frame_once();
                }
                if ui.button("Replay CAN Export").clicked() {
                    self.start_can_export_replay();
                }
            });
            ui.horizontal(|ui| {
                ui.label("CAN Export");
                ui.text_edit_singleline(&mut self.can_export_path);
            });
            ui.horizontal(|ui| {
                ui.label("Interval ms");
                ui.text_edit_singleline(&mut self.interval_ms);
                ui.label("Duration s");
                ui.text_edit_singleline(&mut self.duration_secs);
                if self.is_running() {
                    if ui.button("Stop").clicked() {
                        self.stop_worker();
                    }
                } else if ui.button("Start Send").clicked() {
                    self.start_worker();
                }
            });
            ui.label(format!("status: {}", self.status));
            ui.label(format!("sent_frames: {}", self.sent_frames));
            ui.label(format!("ack_frames: {}", self.ack_frames));
            ui.label(format!("last_req: {}", self.last_req));
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.label("Logs");
            egui::ScrollArea::vertical().stick_to_bottom(true).show(ui, |ui| {
                for line in &self.logs {
                    ui.monospace(line);
                }
            });
        });

        ctx.request_repaint_after(Duration::from_millis(100));
    }
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions::default();
    let app_creator = move |cc: &eframe::CreationContext<'_>| -> Result<Box<dyn eframe::App>, Box<dyn std::error::Error + Send + Sync>> {
        setup_chinese_fonts(&cc.egui_ctx);
        Ok(Box::new(SerialSenderUiApp::new()))
    };
    eframe::run_native("serial_sender_ui", options, Box::new(app_creator))
}
