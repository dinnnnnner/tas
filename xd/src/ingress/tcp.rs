use crate::bus::{EventBus, Store};
use crate::protocol::{FrameCodec, SimpleFrameCodec};
use crate::ingress::serial::publish_status;
use crate::session::{DeviceSession, DeviceSessionHandle, SessionConfig};
use crate::transport::ConnectedTcpTransport;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

pub async fn run_tcp_ingress(
    addr: &str,
    max_payload: usize,
    bus: EventBus,
    store: Store,
    sessions: Arc<RwLock<HashMap<String, DeviceSessionHandle>>>,
) -> std::io::Result<()> {
    let codec: Arc<dyn FrameCodec> = Arc::new(SimpleFrameCodec::new(max_payload));
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (socket, peer) = listener.accept().await?;
        let device_id = format!("tcp://{peer}");
        publish_status(&bus, format!("connected: {peer}"));

        let handle = DeviceSession::spawn(
            device_id.clone(),
            Box::new(ConnectedTcpTransport::new(socket)),
            codec.clone(),
            bus.clone(),
            store.clone(),
            SessionConfig {
                enable_heartbeat: false,
                reconnect_enabled: false,
                ..SessionConfig::default()
            },
        );
        sessions.write().await.insert(device_id, handle);
    }
}
