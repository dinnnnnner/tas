use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

pub type DeviceId = String;
pub type RequestId = u64;

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            base_delay: Duration::from_millis(100),
        }
    }
}

#[derive(Clone, Debug)]
pub enum CommandKind {
    ReadParam,
    WriteParam,
    Control,
    Custom(u8),
}

impl CommandKind {
    pub fn code(&self) -> u8 {
        match self {
            Self::ReadParam => 0x01,
            Self::WriteParam => 0x02,
            Self::Control => 0x03,
            Self::Custom(v) => *v,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Command {
    pub request_id: RequestId,
    pub kind: CommandKind,
    pub payload: Vec<u8>,
    pub timeout: Duration,
    pub retry: RetryPolicy,
    pub idempotent: bool,
}

#[derive(Clone, Debug)]
pub struct Response {
    pub request_id: RequestId,
    pub code: u8,
    pub payload: Vec<u8>,
    pub ts: SystemTime,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ConnState {
    Disconnected,
    Connecting,
    Handshaking,
    Ready,
    Degraded,
    Reconnecting,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct DeviceSnapshot {
    pub device_id: DeviceId,
    pub conn_state: ConnState,
    pub last_seen: Option<SystemTime>,
    pub telemetry: BTreeMap<String, serde_json::Value>,
    pub alarms_active: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AlarmLevel {
    Info,
    Warning,
    Critical,
    Purple,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AlarmEvent {
    pub device_id: DeviceId,
    pub alarm_id: String,
    pub level: AlarmLevel,
    pub message: String,
    pub raised_at: SystemTime,
    pub cleared: bool,
}
impl Default for ConnState {
    fn default() -> Self {
        Self::Disconnected
    }
}
