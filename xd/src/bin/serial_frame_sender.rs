use anyhow::{Context, bail};
use bytes::BytesMut;
use demo2::protocol::{Frame, FrameCodec, SentFrameCodec, SimpleFrameCodec};
use serialport::FlowControl;
use std::thread;
use std::time::{Duration, Instant};
use tokio::time::{self, MissedTickBehavior};

const DEFAULT_PORT: &str = "COM4";
const DEFAULT_BAUD: u32 = 2_000_000;
const DEFAULT_SENSOR_COUNT: usize = 4;
const DEFAULT_INTERVAL_MS: u64 = 30;
const DEFAULT_DURATION_SECS: u64 = 60;
const DEFAULT_KIND: u8 = 0x34;
const DEFAULT_MAX_PAYLOAD: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputFormat {
    Legacy,
    Sent1,
    Sent2,
    Sent3,
}

impl OutputFormat {
    fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "legacy" => Some(Self::Legacy),
            "sent1" => Some(Self::Sent1),
            "sent2" => Some(Self::Sent2),
            "sent3" | "sent539" => Some(Self::Sent3),
            _ => None,
        }
    }

    fn frame_us(self) -> Option<u64> {
        match self {
            Self::Legacy => None,
            Self::Sent1 => Some(439),
            Self::Sent2 => Some(484),
            Self::Sent3 => Some(539),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Sent1 => "SENT1",
            Self::Sent2 => "SENT2",
            Self::Sent3 => "SENT3",
        }
    }
}

struct Config {
    port: String,
    baud: u32,
    sensor_count: usize,
    interval_ms: u64,
    duration_secs: u64,
    format: OutputFormat,
}

fn print_help() {
    println!("serial_frame_sender");
    println!("  --port <COMx>           serial port name, default COM4");
    println!("  --baud <n>              baud rate, default 2000000");
    println!("  --format <mode>         legacy | sent1 | sent2 | sent3, default sent1");
    println!("  --sensors <n>           virtual pair count, default 4");
    println!("  --interval-ms <n>       legacy send interval, default 30");
    println!("  --duration-secs <n>     duration, default 60");
    println!();
    println!("env override:");
    println!("  DEMO2_SERIAL_PORT");
    println!("  DEMO2_SERIAL_BAUD");
    println!("  DEMO2_SERIAL_SENSORS");
    println!("  DEMO2_SERIAL_INTERVAL_MS");
    println!("  DEMO2_SERIAL_DURATION_SECS");
    println!("  DEMO2_SERIAL_FORMAT");
}

fn parse_args() -> anyhow::Result<Config> {
    let mut cfg = Config {
        port: std::env::var("DEMO2_SERIAL_PORT").unwrap_or_else(|_| DEFAULT_PORT.to_string()),
        baud: std::env::var("DEMO2_SERIAL_BAUD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_BAUD),
        sensor_count: std::env::var("DEMO2_SERIAL_SENSORS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SENSOR_COUNT),
        interval_ms: std::env::var("DEMO2_SERIAL_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_INTERVAL_MS),
        duration_secs: std::env::var("DEMO2_SERIAL_DURATION_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_DURATION_SECS),
        format: std::env::var("DEMO2_SERIAL_FORMAT")
            .ok()
            .and_then(|v| OutputFormat::parse(&v))
            .unwrap_or(OutputFormat::Sent1),
    };

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--port" => cfg.port = args.next().context("missing value for --port")?,
            "--baud" => {
                cfg.baud = args
                    .next()
                    .context("missing value for --baud")?
                    .parse()
                    .context("invalid --baud")?
            }
            "--format" => {
                let value = args.next().context("missing value for --format")?;
                cfg.format = OutputFormat::parse(&value)
                    .with_context(|| format!("invalid --format: {value}"))?;
            }
            "--sensors" => {
                cfg.sensor_count = args
                    .next()
                    .context("missing value for --sensors")?
                    .parse()
                    .context("invalid --sensors")?
            }
            "--interval-ms" => {
                cfg.interval_ms = args
                    .next()
                    .context("missing value for --interval-ms")?
                    .parse()
                    .context("invalid --interval-ms")?
            }
            "--duration-secs" => {
                cfg.duration_secs = args
                    .next()
                    .context("missing value for --duration-secs")?
                    .parse()
                    .context("invalid --duration-secs")?
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    if cfg.port.trim().is_empty() {
        bail!("serial port cannot be empty");
    }
    if cfg.baud == 0 {
        bail!("baud must be > 0");
    }
    if cfg.sensor_count == 0 {
        bail!("sensor count must be > 0");
    }
    if cfg.format == OutputFormat::Legacy && cfg.interval_ms == 0 {
        bail!("interval must be > 0 in legacy mode");
    }

    Ok(cfg)
}

fn sensor_value(sensor_id: usize, t_sec: f64) -> f64 {
    let sid = sensor_id as f64;
    let amplitude = 420.0 + (sensor_id % 4) as f64 * 45.0;
    let phase = sid * 0.52;
    let slow_wave = ((2.0 * std::f64::consts::PI * t_sec / 3.0) + phase).sin() * amplitude;
    let ripple_freq = (0.05_f64).max((0.8 + sid * 0.08) / 80.0);
    let ripple = ((2.0 * std::f64::consts::PI * ripple_freq * t_sec) + phase * 2.0).sin()
        * amplitude
        * 0.08;
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
        OutputFormat::Sent1 => SentFrameCodec::with_crc(status, channel_1, channel_2, 0x1),
        OutputFormat::Sent2 => SentFrameCodec::with_crc(status, channel_1, channel_2, 0x6),
        OutputFormat::Sent3 => {
            let rolling_counter = ((frame_index + pair_id as u64) & 0x0F) as u8;
            SentFrameCodec::sent3_with_crc(status, channel_1, rolling_counter, 0x0B)
        }
        OutputFormat::Legacy => SentFrameCodec::with_crc(status, channel_1, channel_2, 0x0),
    }
}

fn drain_ack(
    port: &mut dyn serialport::SerialPort,
    codec: &SimpleFrameCodec,
    ack_buf: &mut BytesMut,
    chunk: &mut [u8],
) -> anyhow::Result<u64> {
    let mut ack_count = 0_u64;

    loop {
        match port.read(chunk) {
            Ok(0) => break,
            Ok(n) => ack_buf.extend_from_slice(&chunk[..n]),
            Err(err) if err.kind() == std::io::ErrorKind::TimedOut => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err.into()),
        }

        loop {
            match codec.try_decode(ack_buf) {
                Ok(Some(frame)) => {
                    ack_count = ack_count.saturating_add(1);
                    let payload = String::from_utf8_lossy(&frame.payload);
                    println!(
                        "ack request_id={} kind=0x{:02X} payload={}",
                        frame.request_id, frame.kind, payload
                    );
                }
                Ok(None) => break,
                Err(err) => {
                    eprintln!("ack decode error: {err}");
                    break;
                }
            }
        }
    }

    Ok(ack_count)
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
                thread::sleep(Duration::from_millis(2));
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
                thread::sleep(Duration::from_millis(2));
            }
            Err(err) => return Err(err.into()),
        }
    }

    port.flush()?;
    Ok(())
}

fn run_legacy_sender(
    cfg: &Config,
    port: &mut dyn serialport::SerialPort,
) -> anyhow::Result<()> {
    let codec = SimpleFrameCodec::new(DEFAULT_MAX_PAYLOAD);
    let total_ticks = (cfg.duration_secs * 1000) / cfg.interval_ms;
    let start = Instant::now();
    let mut req_id = 10_000_u64;
    let mut sent_frames = 0_u64;
    let mut ack_frames = 0_u64;
    let mut ack_buf = BytesMut::with_capacity(1024);
    let mut ack_chunk = [0u8; 256];

    println!(
        "serial sender start: mode=legacy, port={}, baud={}, sensors={}, interval={}ms, duration={}s",
        cfg.port, cfg.baud, cfg.sensor_count, cfg.interval_ms, cfg.duration_secs
    );

    for tick in 0..total_ticks {
        for sensor_id in 0..cfg.sensor_count {
            let t_sec = start.elapsed().as_secs_f64();
            let value = sensor_value(sensor_id, t_sec);
            let payload = format!("sid={sensor_id},value={value:.3}").into_bytes();
            let frame = Frame {
                request_id: req_id,
                kind: DEFAULT_KIND,
                payload,
            };
            req_id = req_id.saturating_add(1);

            let mut out = BytesMut::new();
            codec.encode(&frame, &mut out)?;
            write_frame_all(port, &out).with_context(|| {
                format!("serial write failed on {} for req_id={}", cfg.port, frame.request_id)
            })?;
            sent_frames = sent_frames.saturating_add(1);

            if tick % 10 == 0 {
                println!(
                    "tick={} sensor={} t={:.1}s value={:.3} req_id={}",
                    tick, sensor_id, t_sec, value, frame.request_id
                );
            }
        }

        ack_frames = ack_frames.saturating_add(drain_ack(
            port,
            &codec,
            &mut ack_buf,
            &mut ack_chunk,
        )?);

        thread::sleep(Duration::from_millis(cfg.interval_ms));
    }

    ack_frames = ack_frames.saturating_add(drain_ack(
        port,
        &codec,
        &mut ack_buf,
        &mut ack_chunk,
    )?);

    println!(
        "done in {:.2}s, sent_frames={}, ack_frames={}",
        start.elapsed().as_secs_f64(),
        sent_frames,
        ack_frames
    );
    Ok(())
}

async fn run_sent_sender(
    cfg: &Config,
    port: &mut dyn serialport::SerialPort,
) -> anyhow::Result<()> {
    let frame_us = cfg
        .format
        .frame_us()
        .context("SENT mode requires a fixed frame length")?;
    let start = Instant::now();
    let deadline = start + Duration::from_secs(cfg.duration_secs);
    let mut ticker = time::interval(Duration::from_micros(frame_us));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut frame_index = 0_u64;
    println!(
        "serial sender start: mode={}, port={}, baud={}, pairs={}, t_frame={}us, duration={}s",
        cfg.format.label(),
        cfg.port,
        cfg.baud,
        cfg.sensor_count,
        frame_us,
        cfg.duration_secs
    );

    while Instant::now() < deadline {
        ticker.tick().await;
        let t_sec = start.elapsed().as_secs_f64();

        for pair_id in 0..cfg.sensor_count {
            let sent = build_sent_frame(pair_id, frame_index, t_sec, cfg.format);
            let raw = SentFrameCodec::encode_frame(sent);
            write_frame_all(port, &raw).with_context(|| {
                format!(
                    "serial SENT write failed on {} for frame={} pair={}",
                    cfg.port, frame_index, pair_id
                )
            })?;
            frame_index = frame_index.saturating_add(1);

            if frame_index <= 5 || frame_index % 200 == 0 {
                if cfg.format == OutputFormat::Sent3 {
                    let rolling_counter = (sent.channel_2 & 0x0F) as u8;
                    let inverted_copy = ((sent.channel_2 >> 4) & 0x0F) as u8;
                    let check_nibble = ((sent.channel_2 >> 8) & 0x0F) as u8;
                    println!(
                        "{} frame={} pair={} status=0x{:X} angle=0x{:03X} rc=0x{:X} inv=0x{:X} chk=0x{:X} crc=0x{:X} pause=0x{:X} raw={:02X?}",
                        cfg.format.label(),
                        frame_index,
                        pair_id,
                        sent.status,
                        sent.channel_1,
                        rolling_counter,
                        inverted_copy,
                        check_nibble,
                        sent.crc,
                        sent.pause,
                        raw
                    );
                } else {
                    println!(
                        "{} frame={} pair={} status=0x{:X} p1=0x{:03X} p2=0x{:03X} crc=0x{:X} pause=0x{:X} raw={:02X?}",
                        cfg.format.label(),
                        frame_index,
                        pair_id,
                        sent.status,
                        sent.channel_1,
                        sent.channel_2,
                        sent.crc,
                        sent.pause,
                        raw
                    );
                }
            }
        }
    }

    println!(
        "done in {:.2}s, sent_frames={}, mode={}",
        start.elapsed().as_secs_f64(),
        frame_index,
        cfg.format.label()
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let cfg = parse_args()?;
    let mut port = serialport::new(&cfg.port, cfg.baud)
        .timeout(Duration::from_millis(20))
        .flow_control(FlowControl::None)
        .data_bits(serialport::DataBits::Eight)
        .parity(serialport::Parity::None)
        .stop_bits(serialport::StopBits::One)
        .open()
        .with_context(|| format!("open serial port failed: {} @ {}", cfg.port, cfg.baud))?;
    let _ = port.write_data_terminal_ready(true);
    let _ = port.write_request_to_send(true);
    thread::sleep(Duration::from_millis(100));

    match cfg.format {
        OutputFormat::Legacy => run_legacy_sender(&cfg, &mut *port),
        _ => run_sent_sender(&cfg, &mut *port).await,
    }
}
