use anyhow::{Context, bail};
use bytes::{BufMut, BytesMut};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

const MAGIC: u16 = 0xAA55;
const DEFAULT_KIND: u8 = 0x34;
const DEFAULT_INGRESS_ADDR: &str = "127.0.0.1:19010";
const DEFAULT_EXPORT_PATH: &str =
    "C:\\Users\\Admin\\Desktop\\xd\\exports\\can_export_04-17_09-48-45_04-16_16-43-34_04-17_09-48-34.txt";
const DEFAULT_REPLAY_WINDOW_MS: i64 = 30_000;
const CONNECT_MAX_RETRIES: u32 = 30;
const CONNECT_RETRY_MS: u64 = 300;

#[derive(Clone, Debug)]
struct ReplayRow {
    ts_ms: i64,
    value: f64,
    request_id: u64,
}

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

fn parse_replay_file(path: &str) -> anyhow::Result<Vec<ReplayRow>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read export failed: {path}"))?;
    let mut rows = Vec::new();

    for (line_no, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with("ts_ms\t") {
            continue;
        }

        let cols = line.split('\t').collect::<Vec<_>>();
        if cols.len() < 5 {
            bail!("invalid export row at line {}: {}", line_no + 1, line);
        }
        if cols[2].trim() != "y" {
            continue;
        }

        let ts_ms = cols[0]
            .parse::<i64>()
            .with_context(|| format!("invalid ts_ms at line {}", line_no + 1))?;
        let value = cols[3]
            .parse::<f64>()
            .with_context(|| format!("invalid value at line {}", line_no + 1))?;
        let request_id = cols[4]
            .parse::<u64>()
            .with_context(|| format!("invalid request_id at line {}", line_no + 1))?;

        rows.push(ReplayRow {
            ts_ms,
            value,
            request_id,
        });
    }

    rows.sort_by_key(|row| row.ts_ms);
    Ok(rows)
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

fn run_replay(ingress_addr: &str, export_path: &str) -> anyhow::Result<u64> {
    let mut rows = parse_replay_file(export_path)?;
    if rows.is_empty() {
        bail!("no y-axis rows found in export: {export_path}");
    }

    let window_start_ts_ms = rows[0].ts_ms;
    let window_end_ts_ms = window_start_ts_ms + DEFAULT_REPLAY_WINDOW_MS;
    rows.retain(|row| row.ts_ms <= window_end_ts_ms);
    if rows.is_empty() {
        bail!("no rows remain in the first 30-second replay window: {export_path}");
    }

    println!(
        "replay start: ingress={}, export={}, y_rows={}, window_ms={}, ts_range=[{}, {}]",
        ingress_addr,
        export_path,
        rows.len(),
        DEFAULT_REPLAY_WINDOW_MS,
        window_start_ts_ms,
        window_end_ts_ms
    );

    let mut stream = connect_with_retry(ingress_addr).with_context(|| {
        format!(
            "connect demo2 ingress failed after {} retries: {}, please start `collector_service`",
            CONNECT_MAX_RETRIES, ingress_addr
        )
    })?;
    stream.set_read_timeout(Some(Duration::from_millis(100)))?;

    let start = Instant::now();
    let mut sent_frames = 0_u64;
    let mut last_ts_ms = rows[0].ts_ms;

    for (idx, row) in rows.into_iter().enumerate() {
        let delay_ms = (row.ts_ms - last_ts_ms).max(0) as u64;
        if delay_ms > 0 {
            thread::sleep(Duration::from_millis(delay_ms));
        }
        last_ts_ms = row.ts_ms;

        let payload = format!("sid=0,value={:.3}", row.value).into_bytes();
        let frame = encode_frame(row.request_id, DEFAULT_KIND, &payload);
        stream.write_all(&frame)?;

        let mut ack = [0u8; 128];
        let _ = stream.read(&mut ack);

        sent_frames = sent_frames.saturating_add(1);
        if sent_frames <= 5 || sent_frames % 200 == 0 {
            println!(
                "replay row={} source_ts_ms={} value={:.3} req_id={}",
                idx + 1,
                row.ts_ms,
                row.value,
                row.request_id
            );
        }
    }

    println!(
        "replay done in {:.2}s, sent_frames={}",
        start.elapsed().as_secs_f64(),
        sent_frames
    );
    Ok(sent_frames)
}

fn main() -> anyhow::Result<()> {
    let ingress_addr =
        std::env::var("DEMO2_INGRESS_ADDR").unwrap_or_else(|_| DEFAULT_INGRESS_ADDR.to_string());
    let export_path =
        std::env::var("DEMO2_EXPORT_PATH").unwrap_or_else(|_| DEFAULT_EXPORT_PATH.to_string());

    let sent_frames = run_replay(&ingress_addr, &export_path)?;
    if sent_frames == 0 {
        bail!("no frames sent");
    }
    Ok(())
}
