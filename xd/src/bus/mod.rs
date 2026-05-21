use crate::domain::{AlarmEvent, ConnState, DeviceId, DeviceSnapshot, RequestId};
use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub enum TelemetrySourceKind {
    #[default]
    Unknown,
    SerialDemo,
    SerialSent1,
    SerialSent2,
    SerialSent3,
    CanAxis,
    CanSent,
    TcpFrame,
    FrameStream,
}

/// Device domain events emitted by session/transport/app layers.
#[derive(Clone, Debug)]
pub enum DeviceEvent {
    /// Connection state transition for a specific device.
    ConnStateChanged {
        device_id: DeviceId,
        from: ConnState,
        to: ConnState,
    },
    /// Full snapshot update (coarser-grained state sync).
    TelemetryUpdated {
        device_id: DeviceId,
        snapshot: DeviceSnapshot,
    },
    /// Single telemetry sample (fine-grained stream for charts/alerts).
    TelemetrySample {
        device_id: DeviceId,
        sensor_id: usize,
        t_sec: f64,
        value: f64,
        req_id: RequestId,
        alarm_bit: bool,
        source_kind: TelemetrySourceKind,
    },
    /// Alarm lifecycle events.
    AlarmRaised(AlarmEvent),
    AlarmCleared(AlarmEvent),
    /// Command execution result.
    CommandResult {
        device_id: DeviceId,
        request_id: RequestId,
        ok: bool,
    },
    /// Structured runtime log for UI/diagnostics.
    Log {
        device_id: DeviceId,
        level: &'static str,
        msg: String,
    },
}

/// App-level event envelope.
#[derive(Clone, Debug)]
pub enum AppEvent {
    Device(DeviceEvent),
    System(String),
}

/// In-process pub/sub bus.
///
/// Note: broadcast channel is bounded; if a subscriber is too slow,
/// it will lag and may lose older messages.
#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<AppEvent>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn publish(&self, evt: AppEvent) {
        let _ = self.tx.send(evt);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AppEvent> {
        self.tx.subscribe()
    }
}

/// Lightweight state store for latest device snapshots.
#[derive(Clone, Default)]
pub struct Store {
    snapshots: Arc<RwLock<HashMap<DeviceId, DeviceSnapshot>>>,
}

impl Store {
    pub async fn upsert_snapshot(&self, snapshot: DeviceSnapshot) {
        self.snapshots
            .write()
            .await
            .insert(snapshot.device_id.clone(), snapshot);
    }

    pub async fn snapshot(&self, device_id: &str) -> Option<DeviceSnapshot> {
        self.snapshots.read().await.get(device_id).cloned()
    }
}
