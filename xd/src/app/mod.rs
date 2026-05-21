pub mod alarm_service;

use crate::bus::{AppEvent, EventBus, Store};
use crate::domain::{Command, DeviceId, Response};
use crate::protocol::{FrameCodec, SimpleFrameCodec};
use crate::session::{DeviceSession, DeviceSessionHandle, SessionConfig, SessionError};
use crate::transport::{SerialTransport, TcpTransport};
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;

pub use alarm_service::{AlarmRule, AlarmService};

#[derive(Clone)]
pub struct DeviceManager {
    bus: EventBus,
    store: Store,
    sessions: Arc<DashMap<DeviceId, DeviceSessionHandle>>,
}

impl DeviceManager {
    pub fn new(bus: EventBus, store: Store) -> Self {
        Self {
            bus,
            store,
            sessions: Arc::new(DashMap::new()),
        }
    }

    pub fn event_bus(&self) -> EventBus {
        self.bus.clone()
    }

    pub fn store(&self) -> Store {
        self.store.clone()
    }

    pub fn start_tcp_device(&self, device_id: DeviceId, addr: SocketAddr) {
        let transport = Box::new(TcpTransport::new(addr));
        let codec: Arc<dyn FrameCodec> = Arc::new(SimpleFrameCodec::new(4096));
        let handle = DeviceSession::spawn(
            device_id.clone(),
            transport,
            codec,
            self.bus.clone(),
            self.store.clone(),
            SessionConfig::default(),
        );
        self.sessions.insert(device_id, handle);
        self.bus.publish(AppEvent::System("device started".to_string()));
    }

    pub fn start_serial_device(&self, device_id: DeviceId, port_name: impl Into<String>, baud_rate: u32) {
        let transport = Box::new(SerialTransport::new(port_name, baud_rate));
        let codec: Arc<dyn FrameCodec> = Arc::new(SimpleFrameCodec::new(4096));
        let handle = DeviceSession::spawn(
            device_id.clone(),
            transport,
            codec,
            self.bus.clone(),
            self.store.clone(),
            SessionConfig {
                enable_heartbeat: false,
                reconnect_enabled: false,
                ..SessionConfig::default()
            },
        );
        self.sessions.insert(device_id, handle);
        self.bus.publish(AppEvent::System("serial device started".to_string()));
    }

    pub async fn call(&self, device_id: &str, cmd: Command) -> Result<Response, SessionError> {
        let Some(handle) = self.sessions.get(device_id) else {
            return Err(SessionError::Closed);
        };
        handle.call(cmd).await
    }

    pub async fn stop_device(&self, device_id: &str) {
        if let Some((_, handle)) = self.sessions.remove(device_id) {
            handle.stop().await;
        }
    }
}

#[derive(Clone)]
pub struct CommandService {
    manager: DeviceManager,
}

impl CommandService {
    pub fn new(manager: DeviceManager) -> Self {
        Self { manager }
    }

    pub async fn call(&self, device_id: &str, cmd: Command) -> Result<Response, SessionError> {
        self.manager.call(device_id, cmd).await
    }
}
