use anyhow::Context;
use bytes::{BufMut, BytesMut};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessesToUpdate, System};

const MAGIC: u16 = 0xAA55;
const SENSOR_COUNT: usize = 10;
const DEFAULT_INGRESS_ADDR: &str = "127.0.0.1:19010";
const CONNECT_MAX_RETRIES: u32 = 30;
const CONNECT_RETRY_MS: u64 = 300;

fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0xFFFF;
    for b in data {
        crc ^= *b as u16;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xA001;
            } else {
                crc >>= 1;
            }
        }
    }
    crc
}

fn encode_frame(request_id: u64, kind: u8, payload: &[u8]) -> Vec<u8> {
    let body_len = 8 + 1 + payload.len();
    let mut out = BytesMut::with_capacity(2 + 2 + body_len + 2);
    out.put_u16(MAGIC);
    out.put_u16(body_len as u16);
    out.put_u64(request_id);
    out.put_u8(kind);
    out.extend_from_slice(payload);
    let crc = crc16(&out[4..]);
    out.put_u16(crc);
    out.to_vec()
}

fn sensor_value(sensor_id: usize, t_sec: f64) -> f64 {
    let sid = sensor_id as f64;
    let base = 30.0 + sid * 6.5;
    let freq = 0.45 + sid * 0.06;
    let amp = 8.0 + (sensor_id % 5) as f64 * 3.2;
    let phase = sid * 0.72;
    let harmonic = (t_sec * (freq * 1.9) + phase * 0.6).cos() * (amp * 0.22);
    let drift = ((t_sec / 45.0) + sid * 0.11).sin() * 2.0;
    base + (t_sec * freq + phase).sin() * amp + harmonic + drift
}

fn read_ack(stream: &mut TcpStream) -> io::Result<()> {
    let mut head = [0u8; 4];
    stream.read_exact(&mut head)?;

    let magic = u16::from_be_bytes([head[0], head[1]]);
    if magic != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }

    let body_len = u16::from_be_bytes([head[2], head[3]]) as usize;
    if body_len < 9 || body_len > 8192 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad body length"));
    }

    let mut body_crc = vec![0u8; body_len + 2];
    stream.read_exact(&mut body_crc)?;

    let body = &body_crc[..body_len];
    let recv_crc = u16::from_be_bytes([body_crc[body_len], body_crc[body_len + 1]]);
    let calc_crc = crc16(body);
    if recv_crc != calc_crc {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad crc"));
    }

    Ok(())
}

fn process_mem_mb(sys: &mut System) -> f64 {
    let pid = Pid::from_u32(std::process::id());
    sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    if let Some(proc_) = sys.process(pid) {
        proc_.memory() as f64 / 1024.0 / 1024.0
    } else {
        0.0
    }
}

fn connect_with_retry(ingress_addr: &str) -> anyhow::Result<TcpStream> {
    let mut last_err: Option<std::io::Error> = None;
    for attempt in 1..=CONNECT_MAX_RETRIES {
        match TcpStream::connect(ingress_addr) {
            Ok(stream) => {
                if attempt > 1 {
                    println!("connected after retry attempt {}", attempt);
                }
                return Ok(stream);
            }
            Err(err) => {
                last_err = Some(err);
                if attempt < CONNECT_MAX_RETRIES {
                    thread::sleep(Duration::from_millis(CONNECT_RETRY_MS));
                }
            }
        }
    }
    let err = last_err.unwrap_or_else(|| std::io::Error::other("unknown connect error"));
    Err(anyhow::Error::from(err))
}

#[derive(Debug)]
struct StageResult {
    target_tick_hz: u64,
    target_frame_hz: u64,
    duration_s: f64,
    ticks: u64,
    sent_frames: u64,
    ack_ok: u64,
    ack_fail: u64,
    actual_tick_hz: f64,
    actual_frame_hz: f64,
    loss_rate: f64,
    mem_mb: f64,
}

fn run_stage(
    stream: &mut TcpStream,
    target_tick_hz: u64,
    stage_secs: u64,
    req_id_seed: &mut u64,
    sys: &mut System,
) -> StageResult {
    let interval = Duration::from_micros((1_000_000 / target_tick_hz).max(1));
    let stage_start = Instant::now();

    let mut ticks = 0_u64;
    let mut sent_frames = 0_u64;
    let mut ack_ok = 0_u64;
    let mut ack_fail = 0_u64;

    while stage_start.elapsed() < Duration::from_secs(stage_secs) {
        let t = stage_start.elapsed().as_secs_f64();

        for sid in 0..SENSOR_COUNT {
            let value = sensor_value(sid, t);
            let payload = format!("sid={sid},value={value:.3}").into_bytes();

            let req_id = *req_id_seed;
            *req_id_seed = req_id.saturating_add(1);

            let frame = encode_frame(req_id, 0x34, &payload);
            if stream.write_all(&frame).is_ok() {
                sent_frames = sent_frames.saturating_add(1);
                match read_ack(stream) {
                    Ok(()) => ack_ok = ack_ok.saturating_add(1),
                    Err(_) => ack_fail = ack_fail.saturating_add(1),
                }
            } else {
                ack_fail = ack_fail.saturating_add(1);
            }
        }

        ticks = ticks.saturating_add(1);
        thread::sleep(interval);
    }

    let duration_s = stage_start.elapsed().as_secs_f64();
    let actual_tick_hz = if duration_s > 0.0 {
        ticks as f64 / duration_s
    } else {
        0.0
    };
    let actual_frame_hz = if duration_s > 0.0 {
        sent_frames as f64 / duration_s
    } else {
        0.0
    };
    let loss_rate = if sent_frames > 0 {
        ack_fail as f64 / sent_frames as f64 * 100.0
    } else {
        0.0
    };

    StageResult {
        target_tick_hz,
        target_frame_hz: target_tick_hz * SENSOR_COUNT as u64,
        duration_s,
        ticks,
        sent_frames,
        ack_ok,
        ack_fail,
        actual_tick_hz,
        actual_frame_hz,
        loss_rate,
        mem_mb: process_mem_mb(sys),
    }
}

fn main() -> anyhow::Result<()> {
    let ingress_addr =
        std::env::var("DEMO2_INGRESS_ADDR").unwrap_or_else(|_| DEFAULT_INGRESS_ADDR.to_string());
    println!("connecting demo2 ingress: {ingress_addr}");
    let mut stream = connect_with_retry(&ingress_addr).with_context(|| {
        format!(
            "connect demo2 ingress failed after {} retries: {}, please start `collector_service`",
            CONNECT_MAX_RETRIES, ingress_addr
        )
    })?;
    stream
        .set_read_timeout(Some(Duration::from_millis(120)))
        .context("set read timeout failed")?;

    let mut sys = System::new_all();
    let mut req = 50_000_u64;

    let stages_tick_hz = [2_u64, 5, 10, 20, 40, 80];
    let stage_secs = 8_u64;

    println!("=== sender stress report start ===");
    println!(
        "sensor_count={}, target_tick_hz={:?}, each {}s",
        SENSOR_COUNT, stages_tick_hz, stage_secs
    );

    let mut results = Vec::new();
    for tick_hz in stages_tick_hz {
        let r = run_stage(&mut stream, tick_hz, stage_secs, &mut req, &mut sys);
        println!(
            "stage target={}tick/s({}frame/s) actual={:.1}tick/s({:.1}frame/s) sent={} ack_ok={} ack_fail={} loss={:.2}% mem={:.2}MB",
            r.target_tick_hz,
            r.target_frame_hz,
            r.actual_tick_hz,
            r.actual_frame_hz,
            r.sent_frames,
            r.ack_ok,
            r.ack_fail,
            r.loss_rate,
            r.mem_mb
        );
        results.push(r);
        thread::sleep(Duration::from_millis(400));
    }

    let best = results.iter().filter(|r| r.loss_rate <= 1.0).max_by(|a, b| {
        a.actual_frame_hz
            .partial_cmp(&b.actual_frame_hz)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    println!("\n=== summary ===");
    for r in &results {
        println!(
            "target={:>3}tick/s({:>4}f/s) | actual={:>6.1}tick/s({:>7.1}f/s) | sent={:>6} | ticks={:>5} | loss={:>6.2}% | mem={:>6.2}MB | duration={:>4.1}s",
            r.target_tick_hz,
            r.target_frame_hz,
            r.actual_tick_hz,
            r.actual_frame_hz,
            r.sent_frames,
            r.ticks,
            r.loss_rate,
            r.mem_mb,
            r.duration_s
        );
    }

    if let Some(b) = best {
        println!(
            "suggested upper bound (loss<=1%): {:.1} frame/s (~{:.1} tick/s, {} sensors)",
            b.actual_frame_hz,
            b.actual_tick_hz,
            SENSOR_COUNT
        );
    } else {
        println!("no stage met loss<=1%, reduce sender rate or optimize receiver path");
    }

    println!("=== sender stress report done ===");
    Ok(())
}
