use crate::bus::{AppEvent, DeviceEvent, EventBus, TelemetrySourceKind};
use crate::domain::{AlarmEvent, AlarmLevel};
use crate::ingress::serial::publish_status;
use crate::transport::can::{CanFrame, CanTransport, CanTransportConfig, CanTransportError, CanTxFrame};
use std::collections::{HashSet, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;
use std::time::SystemTime;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender, error::TryRecvError};

static CAN_TX_SENDER: OnceLock<Mutex<Option<UnboundedSender<CanTxFrame>>>> = OnceLock::new();

pub fn enqueue_can_tx(frame: CanTxFrame) -> Result<(), String> {
    let slot = CAN_TX_SENDER.get_or_init(|| Mutex::new(None));
    let guard = slot.lock().map_err(|_| "CAN TX queue lock poisoned".to_string())?;
    let Some(sender) = guard.as_ref() else {
        return Err("collector CAN channel not ready".to_string());
    };
    sender
        .send(frame)
        .map_err(|err| format!("collector CAN TX enqueue failed: {err}"))
}

fn set_can_tx_sender(sender: Option<UnboundedSender<CanTxFrame>>) -> Result<(), String> {
    let slot = CAN_TX_SENDER.get_or_init(|| Mutex::new(None));
    let mut guard = slot.lock().map_err(|_| "CAN TX queue lock poisoned".to_string())?;
    *guard = sender;
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub struct SentFilterConfig {
    pub enabled: bool,
    pub window_size: usize,
}

impl Default for SentFilterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            window_size: 10,
        }
    }
}

pub async fn run_can_ingress(
    config: CanTransportConfig,
    sent_filter_config: SentFilterConfig,
    bus: EventBus,
) {
    let Some((mut transport, active_config)) = connect_can_transport(config, &bus).await else {
        return;
    };
    let device_id = format!("can://{}:ch{}", active_config.hardware_name, active_config.channel_index);
    let (tx_sender, mut tx_receiver) = mpsc::unbounded_channel();
    let _ = set_can_tx_sender(Some(tx_sender));

    publish_status(
        &bus,
        format!(
            "collector can listening: hw={} ch={} arb={}kbps data={}kbps",
            active_config.hardware_name,
            active_config.channel_index,
            active_config.arbitration_baud_kbps,
            active_config.data_baud_kbps
        ),
    );

    let start = Instant::now();
    let mut frame_count = 0_u64;
    let mut sent_filter = SentMovingAverage::new(sent_filter_config.window_size);
    let mut active_sent_errors = HashSet::new();
    loop {
        drain_tx_queue(
            &mut transport,
            &bus,
            &device_id,
            active_config.channel_index,
            &mut tx_receiver,
        )
        .await;
        match transport.recv().await {
            Ok(Some(frame)) => {
                frame_count = frame_count.saturating_add(1);
                if let Some((x_ok, y_ok, z_ok)) = decode_self_test_response(&frame) {
                    publish_status(
                        &bus,
                        format!(
                            "CAN 自检结果: X={} Y={} Z={}",
                            if x_ok { "通过" } else { "失败" },
                            if y_ok { "通过" } else { "失败" },
                            if z_ok { "通过" } else { "失败" },
                        ),
                    );
                }
                if let Some(error) = decode_sent_error(&frame) {
                    reconcile_sent_error(&bus, &device_id, error, &mut active_sent_errors);
                }
                if let Some(values) = decode_sent_values(&frame) {
                    let output_values = if sent_filter_config.enabled {
                        sent_filter.apply(values)
                    } else {
                        values
                    };
                    for (sensor_id, value) in output_values {
                        bus.publish(AppEvent::Device(DeviceEvent::TelemetrySample {
                            device_id: device_id.clone(),
                            sensor_id,
                            t_sec: start.elapsed().as_secs_f64(),
                            value,
                            req_id: frame_count,
                            alarm_bit: false,
                            source_kind: TelemetrySourceKind::CanSent,
                        }));
                    }
                }
                if let Some((sensor_id, value)) = decode_axis_sample(&frame) {
                    bus.publish(AppEvent::Device(DeviceEvent::TelemetrySample {
                        device_id: device_id.clone(),
                        sensor_id,
                        t_sec: start.elapsed().as_secs_f64(),
                        value,
                        req_id: frame_count,
                        alarm_bit: false,
                        source_kind: TelemetrySourceKind::CanAxis,
                    }));
                }
                if frame_count <= 5 || frame_count % 100 == 0 {
                    let msg = format!(
                        "{} frame={} {}",
                        device_id,
                        frame_count,
                        format_can_frame(&frame)
                    );
                    publish_status(&bus, msg.clone());
                    bus.publish(AppEvent::Device(DeviceEvent::Log {
                        device_id: device_id.clone(),
                        level: "info",
                        msg,
                    }));
                }
            }
            Ok(None) => {}
            Err(CanTransportError::CallbackDisconnected) => {
                publish_status(&bus, format!("collector can callback disconnected {device_id}"));
                break;
            }
            Err(err) => {
                publish_status(&bus, format!("collector can read error {device_id}: {err}"));
                break;
            }
        }
    }

    let _ = set_can_tx_sender(None);
    let _ = transport.close().await;
}

async fn connect_can_transport(
    config: CanTransportConfig,
    bus: &EventBus,
) -> Option<(CanTransport, CanTransportConfig)> {
    let candidates = candidate_can_configs(&config);
    let scanning = candidates.len() > 1;
    let mut failures = Vec::new();

    if scanning {
        publish_status(
            bus,
            format!(
                "collector can auto-detect enabled, probing {} candidate(s)",
                candidates.len()
            ),
        );
    }

    for candidate in candidates {
        let candidate_desc = format_can_target(&candidate);
        let mut transport = CanTransport::new(candidate.clone());
        match transport.connect().await {
            Ok(()) => {
                if candidate.hardware_name != config.hardware_name
                    || candidate.channel_index != config.channel_index
                {
                    publish_status(
                        bus,
                        format!("collector can auto-detected {candidate_desc}"),
                    );
                }
                return Some((transport, candidate));
            }
            Err(err) => {
                if scanning {
                    publish_status(
                        bus,
                        format!("collector can probe failed {candidate_desc}: {err}"),
                    );
                }
                failures.push(format!("{candidate_desc}: {err}"));
            }
        }
    }

    let requested = format_can_target(&config);
    let detail = failures.join(" | ");
    publish_status(
        bus,
        format!("collector can open failed {requested}; tried: {detail}"),
    );
    None
}

fn candidate_can_configs(config: &CanTransportConfig) -> Vec<CanTransportConfig> {
    let mut result = Vec::new();
    let mut seen = HashSet::new();

    let requested_name = config.hardware_name.trim();
    let is_auto_name = requested_name.is_empty() || requested_name.eq_ignore_ascii_case("auto");

    let mut names = Vec::new();
    if !requested_name.is_empty() {
        names.push(requested_name.to_string());
    }
    if is_auto_name || !requested_name.eq_ignore_ascii_case("TC1012") {
        names.push("TC1012".to_string());
    }

    let max_scan_channel = usize::max(4, config.channel_count.max(1) as usize);
    let mut channels = Vec::new();
    channels.push(config.channel_index);
    for channel in 0..max_scan_channel {
        let channel = channel as u8;
        if channel != config.channel_index {
            channels.push(channel);
        }
    }

    for hardware_name in names {
        for &channel_index in &channels {
            let mut candidate = config.clone();
            candidate.hardware_name = hardware_name.clone();
            candidate.channel_index = channel_index;
            candidate.channel_count = i32::from(channel_index) + 1;
            let key = (candidate.hardware_name.clone(), candidate.channel_index);
            if seen.insert(key) {
                result.push(candidate);
            }
        }
    }

    result
}

fn format_can_target(config: &CanTransportConfig) -> String {
    format!("hw={} ch={}", config.hardware_name, config.channel_index)
}

async fn drain_tx_queue(
    transport: &mut CanTransport,
    bus: &EventBus,
    device_id: &str,
    channel_index: u8,
    rx: &mut UnboundedReceiver<CanTxFrame>,
) {
    loop {
        let mut frame = match rx.try_recv() {
            Ok(frame) => frame,
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
        };
        frame.channel = channel_index;
        if let Err(err) = transport.transmit(frame).await {
            publish_status(bus, format!("collector can tx failed {device_id}: {err}"));
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct SentCanError {
    error_type: u8,
    t1_error: bool,
    t2_error: bool,
    s_error: bool,
}

struct SentMovingAverage {
    window_size: usize,
    windows: [VecDeque<f64>; 5],
}

impl SentMovingAverage {
    fn new(window_size: usize) -> Self {
        Self {
            window_size: window_size.max(1),
            windows: std::array::from_fn(|_| VecDeque::with_capacity(window_size.max(1))),
        }
    }

    fn apply(&mut self, values: [(usize, f64); 5]) -> [(usize, f64); 5] {
        values.map(|(sensor_id, value)| {
            let Some(window) = self.windows.get_mut(sensor_id) else {
                return (sensor_id, value);
            };
            window.push_back(value);
            while window.len() > self.window_size {
                let _ = window.pop_front();
            }
            let filtered = if matches!(sensor_id, 0 | 2 | 4) {
                circular_mean_degrees(window, value)
            } else {
                window.iter().sum::<f64>() / window.len() as f64
            };
            (sensor_id, filtered)
        })
    }
}

fn circular_mean_degrees(samples: &VecDeque<f64>, reference: f64) -> f64 {
    if samples.is_empty() {
        return reference;
    }

    let (sin_sum, cos_sum) = samples.iter().fold((0.0, 0.0), |(sin_sum, cos_sum), value| {
        let radians = value.to_radians();
        (sin_sum + radians.sin(), cos_sum + radians.cos())
    });

    let mut mean = sin_sum.atan2(cos_sum).to_degrees();
    while mean - reference > 180.0 {
        mean -= 360.0;
    }
    while reference - mean > 180.0 {
        mean += 360.0;
    }
    mean
}

fn decode_sent_values(frame: &CanFrame) -> Option<[(usize, f64); 5]> {
    if frame.is_tx() || frame.identifier == 3 {
        return None;
    }
    let data = frame.data_bytes();
    if data.len() < 53 {
        return None;
    }

    Some([
        (0, read_f32_le(data, 25)? as f64), // T1 angle
        (1, read_f32_le(data, 29)? as f64), // T1 torque
        (2, read_f32_le(data, 1)? as f64),  // T2 angle
        (3, read_f32_le(data, 5)? as f64),  // T2 torque
        (4, read_f32_le(data, 49)? as f64), // S angle
    ])
}

fn decode_sent_error(frame: &CanFrame) -> Option<SentCanError> {
    if frame.is_tx() || frame.identifier != 3 {
        return None;
    }
    let data = frame.data_bytes();
    if data.len() < 4 {
        return None;
    }
    Some(SentCanError {
        error_type: data[0],
        t1_error: data[1] != 0,
        t2_error: data[2] != 0,
        s_error: data[3] != 0,
    })
}

fn read_f32_le(data: &[u8], offset: usize) -> Option<f32> {
    let bytes: [u8; 4] = data.get(offset..offset + 4)?.try_into().ok()?;
    Some(f32::from_le_bytes(bytes))
}

fn reconcile_sent_error(
    bus: &EventBus,
    device_id: &str,
    error: SentCanError,
    active_errors: &mut HashSet<u8>,
) {
    if error.error_type == 0 {
        for error_type in active_errors.drain().collect::<Vec<_>>() {
            publish_sent_error(bus, device_id, error_type, error, true);
        }
        return;
    }

    if active_errors.insert(error.error_type) {
        publish_sent_error(bus, device_id, error.error_type, error, false);
    }
}

fn publish_sent_error(
    bus: &EventBus,
    device_id: &str,
    error_type: u8,
    error: SentCanError,
    cleared: bool,
) {
    let detail = format!(
        "SENT error type={} t1={} t2={} s={} {}",
        error_type,
        error.t1_error as u8,
        error.t2_error as u8,
        error.s_error as u8,
        if cleared { "cleared" } else { "active" }
    );
    let alarm = AlarmEvent {
        device_id: device_id.to_string(),
        alarm_id: format!("sent_error_{error_type}"),
        level: AlarmLevel::Warning,
        message: detail.clone(),
        raised_at: SystemTime::now(),
        cleared,
    };
    if cleared {
        bus.publish(AppEvent::Device(DeviceEvent::AlarmCleared(alarm)));
    } else {
        bus.publish(AppEvent::Device(DeviceEvent::AlarmRaised(alarm)));
    }
    publish_status(bus, detail);
}

fn decode_axis_sample(frame: &CanFrame) -> Option<(usize, f64)> {
    let sensor_id = match frame.identifier {
        0x100 => 0,
        0x102 => 1,
        0x104 => 2,
        _ => return None,
    };
    let data = frame.data_bytes();
    if data.len() < 4 {
        return None;
    }
    
    let raw = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);

    Some((sensor_id, raw  as f64))
}

fn decode_self_test_response(frame: &CanFrame) -> Option<(bool, bool, bool)> {
    if frame.is_tx() || frame.identifier != 0x123 {
        return None;
    }
    let data = frame.data_bytes();
    if data.len() < 3 {
        return None;
    }
    Some((data[0] == 1, data[1] == 1, data[2] == 1))
}

fn format_can_frame(frame: &CanFrame) -> String {
    let direction = if frame.is_tx() { "TX" } else { "RX" };
    let payload = frame
        .data_bytes()
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "{direction} ch={} id=0x{:03X} dlc={} ts={}us data={payload}",
        frame.channel,
        frame.identifier,
        frame.dlc,
        frame.timestamp_us
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sent_angle_filter_wraps_across_zero_degrees() {
        let mut filter = SentMovingAverage::new(3);

        let first = filter.apply([(0, 359.0), (1, 10.0), (2, 0.0), (3, 20.0), (4, 0.0)]);
        assert!((first[0].1 - 359.0).abs() < 0.001);

        let second = filter.apply([(0, 0.0), (1, 20.0), (2, 0.0), (3, 40.0), (4, 0.0)]);
        assert!(
            second[0].1 > 359.0 || second[0].1 < 1.0,
            "angle average should stay near wrap boundary, got {}",
            second[0].1
        );

        let third = filter.apply([(0, 1.0), (1, 30.0), (2, 0.0), (3, 60.0), (4, 0.0)]);
        assert!(
            third[0].1 > -1.0 && third[0].1 < 2.0,
            "359/0/1 should average near 0 degrees, got {}",
            third[0].1
        );
    }

    #[test]
    fn sent_torque_filter_uses_arithmetic_average() {
        let mut filter = SentMovingAverage::new(3);

        let _ = filter.apply([(0, 0.0), (1, 10.0), (2, 0.0), (3, 20.0), (4, 0.0)]);
        let _ = filter.apply([(0, 0.0), (1, 20.0), (2, 0.0), (3, 40.0), (4, 0.0)]);
        let third = filter.apply([(0, 0.0), (1, 30.0), (2, 0.0), (3, 60.0), (4, 0.0)]);

        assert!((third[1].1 - 20.0).abs() < 0.001);
        assert!((third[3].1 - 40.0).abs() < 0.001);
    }
}
