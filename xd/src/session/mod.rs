use crate::bus::{AppEvent, DeviceEvent, EventBus, Store};
use crate::domain::{Command, ConnState, DeviceId, DeviceSnapshot, Response};
use crate::bus::TelemetrySourceKind;
use crate::protocol::{Frame, FrameCodec};
use crate::transport::{Transport, TransportError};
use bytes::BytesMut;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("session closed")]
    Closed,
    #[error("command timeout")]
    Timeout,
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
    #[error("codec error: {0}")]
    Codec(String),
    #[error("command channel full")]
    Busy,
}

pub struct SessionConfig {
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub reconnect_base: Duration,
    pub reconnect_max: Duration,
    pub enable_heartbeat: bool,
    pub reconnect_enabled: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(2),
            heartbeat_timeout: Duration::from_secs(8),
            reconnect_base: Duration::from_millis(200),
            reconnect_max: Duration::from_secs(5),
            enable_heartbeat: true,
            reconnect_enabled: true,
        }
    }
}

enum SessionMsg {
    Call(Command, oneshot::Sender<Result<Response, SessionError>>),
    Stop,
}

struct PendingRequest {
    cmd: Command,
    responder: oneshot::Sender<Result<Response, SessionError>>,
    deadline: Instant,
    attempts: u32,
}

#[derive(Clone)]
pub struct DeviceSessionHandle {
    device_id: DeviceId,
    tx: mpsc::Sender<SessionMsg>,
}

impl DeviceSessionHandle {
    pub async fn call(&self, cmd: Command) -> Result<Response, SessionError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(SessionMsg::Call(cmd, tx))
            .await
            .map_err(|_| SessionError::Closed)?;
        rx.await.map_err(|_| SessionError::Closed)?
    }

    pub async fn stop(&self) {
        let _ = self.tx.send(SessionMsg::Stop).await;
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }
}

pub struct DeviceSession {
    device_id: DeviceId,
    state: ConnState,
    transport: Box<dyn Transport>,
    codec: Arc<dyn FrameCodec>,
    bus: EventBus,
    store: Store,
    config: SessionConfig,
    rx: mpsc::Receiver<SessionMsg>,
}

impl DeviceSession {
    pub fn spawn(
        device_id: DeviceId,
        transport: Box<dyn Transport>,
        codec: Arc<dyn FrameCodec>,
        bus: EventBus,
        store: Store,
        config: SessionConfig,
    ) -> DeviceSessionHandle {
        let (tx, rx) = mpsc::channel(256);
        let mut session = Self {
            device_id: device_id.clone(),
            state: ConnState::Disconnected,
            transport,
            codec,
            bus,
            store,
            config,
            rx,
        };

        tokio::spawn(async move {
            session.run().await;
        });

        DeviceSessionHandle { device_id, tx }
    }

    async fn run(&mut self) {
        let mut backoff = self.config.reconnect_base;

        loop {
            self.set_state(ConnState::Connecting).await;
            match self.transport.connect().await {
                Ok(()) => {
                    self.set_state(ConnState::Handshaking).await;
                    self.set_state(ConnState::Ready).await;
                    info!(device_id = %self.device_id, "session connected");

                    if self.connection_loop().await.is_ok() {
                        self.set_state(ConnState::Disconnected).await;
                        let _ = self.transport.close().await;
                        break;
                    }
                }
                Err(err) => {
                    warn!(device_id = %self.device_id, error = %err, "connect failed");
                }
            }

            if !self.config.reconnect_enabled {
                self.set_state(ConnState::Disconnected).await;
                let _ = self.transport.close().await;
                break;
            }
            self.set_state(ConnState::Reconnecting).await;
            let _ = self.transport.close().await;
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(self.config.reconnect_max);
        }
    }

    async fn connection_loop(&mut self) -> Result<(), SessionError> {
        let connection_start = Instant::now();
        let mut in_buf = BytesMut::with_capacity(8192);
        let mut pending: HashMap<u64, PendingRequest> = HashMap::new();
        let mut heartbeat_tick = tokio::time::interval(self.config.heartbeat_interval);
        let mut timeout_tick = tokio::time::interval(Duration::from_millis(100));
        let mut last_rx = Instant::now();

        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(SessionMsg::Call(cmd, responder)) => {
                            let frame = Frame { request_id: cmd.request_id, kind: cmd.kind.code(), payload: cmd.payload.clone() };
                            let mut out = BytesMut::with_capacity(64 + frame.payload.len());
                            self.codec.encode(&frame, &mut out).map_err(|e| SessionError::Codec(e.to_string()))?;

                            if let Err(err) = self.transport.write_all(&out).await {
                                let _ = responder.send(Err(SessionError::Transport(err)));
                                return Err(SessionError::Closed);
                            }

                            pending.insert(cmd.request_id, PendingRequest {
                                cmd: cmd.clone(),
                                responder,
                                deadline: Instant::now() + cmd.timeout,
                                attempts: 1,
                            });
                        }
                        Some(SessionMsg::Stop) | None => {
                            for (_, p) in pending.drain() {
                                let _ = p.responder.send(Err(SessionError::Closed));
                            }
                            return Ok(());
                        }
                    }
                }
                _ = heartbeat_tick.tick() => {
                    if !self.config.enable_heartbeat {
                        continue;
                    }
                    let hb = Frame { request_id: 0, kind: 0xF0, payload: Vec::new() };
                    let mut out = BytesMut::with_capacity(32);
                    self.codec.encode(&hb, &mut out).map_err(|e| SessionError::Codec(e.to_string()))?;
                    self.transport.write_all(&out).await?;

                    if Instant::now().duration_since(last_rx) > self.config.heartbeat_timeout {
                        self.set_state(ConnState::Degraded).await;
                        return Err(SessionError::Timeout);
                    }
                }
                _ = timeout_tick.tick() => {
                    let now = Instant::now();
                    let mut timed_out = Vec::new();
                    for (id, p) in &pending {
                        if now >= p.deadline {
                            timed_out.push(*id);
                        }
                    }

                    for id in timed_out {
                        if let Some(mut p) = pending.remove(&id) {
                            if p.cmd.idempotent && p.attempts < p.cmd.retry.max_attempts {
                                p.attempts += 1;
                                p.deadline = Instant::now() + p.cmd.timeout;

                                let retry_frame = Frame {
                                    request_id: p.cmd.request_id,
                                    kind: p.cmd.kind.code(),
                                    payload: p.cmd.payload.clone(),
                                };
                                let mut out = BytesMut::with_capacity(64 + retry_frame.payload.len());
                                self.codec
                                    .encode(&retry_frame, &mut out)
                                    .map_err(|e| SessionError::Codec(e.to_string()))?;
                                self.transport.write_all(&out).await?;
                                pending.insert(id, p);
                            } else {
                                let _ = p.responder.send(Err(SessionError::Timeout));
                                self.bus.publish(AppEvent::Device(DeviceEvent::CommandResult {
                                    device_id: self.device_id.clone(),
                                    request_id: id,
                                    ok: false,
                                }));
                            }
                        }
                    }
                }
                read_res = self.transport.read(&mut in_buf) => {
                    let n = read_res?;
                    if n == 0 {
                        return Err(SessionError::Closed);
                    }
                    last_rx = Instant::now();
                    if self.state == ConnState::Degraded {
                        self.set_state(ConnState::Ready).await;
                    }

                    loop {
                        let maybe = self
                            .codec
                            .try_decode(&mut in_buf)
                            .map_err(|e| SessionError::Codec(e.to_string()))?;
                        let Some(frame) = maybe else { break; };

                        if frame.kind == 0xF1 {
                            continue;
                        }

                        if let Some(pending_req) = pending.remove(&frame.request_id) {
                            let resp = Response {
                                request_id: frame.request_id,
                                code: frame.kind,
                                payload: frame.payload,
                                ts: SystemTime::now(),
                            };
                            let _ = pending_req.responder.send(Ok(resp));
                            self.bus.publish(AppEvent::Device(DeviceEvent::CommandResult {
                                device_id: self.device_id.clone(),
                                request_id: frame.request_id,
                                ok: true,
                            }));
                        } else {
                            if frame.kind == 0x34 {
                                if let Some((sensor_id, value)) = parse_sensor_value(&frame.payload) {
                                    let mut snapshot = self
                                        .store
                                        .snapshot(&self.device_id)
                                        .await
                                        .unwrap_or_else(|| DeviceSnapshot {
                                            device_id: self.device_id.clone(),
                                            ..DeviceSnapshot::default()
                                        });
                                    snapshot.last_seen = Some(SystemTime::now());
                                    snapshot
                                        .telemetry
                                        .insert(format!("sensor_{sensor_id}"), json!(value));
                                    self.store.upsert_snapshot(snapshot).await;

                                    self.bus.publish(AppEvent::Device(DeviceEvent::TelemetrySample {
                                        device_id: self.device_id.clone(),
                                        sensor_id,
                                        t_sec: connection_start.elapsed().as_secs_f64(),
                                        value,
                                        req_id: frame.request_id,
                                        alarm_bit: false,
                                        source_kind: if self.device_id.starts_with("tcp://") {
                                            TelemetrySourceKind::TcpFrame
                                        } else {
                                            TelemetrySourceKind::FrameStream
                                        },
                                    }));

                                    let ack = Frame {
                                        request_id: frame.request_id,
                                        kind: 0x90,
                                        payload: b"demo2-ack".to_vec(),
                                    };
                                    let mut out = BytesMut::new();
                                    self.codec
                                        .encode(&ack, &mut out)
                                        .map_err(|e| SessionError::Codec(e.to_string()))?;
                                    self.transport.write_all(&out).await?;
                                } else {
                                    self.bus.publish(AppEvent::Device(DeviceEvent::Log {
                                        device_id: self.device_id.clone(),
                                        level: "warn",
                                        msg: "invalid telemetry payload for kind=0x34".to_string(),
                                    }));
                                }
                            } else {
                                self.bus.publish(AppEvent::Device(DeviceEvent::Log {
                                    device_id: self.device_id.clone(),
                                    level: "warn",
                                    msg: format!("unsolicited frame kind={}", frame.kind),
                                }));
                            }
                        }
                    }
                }
            }
        }
    }

    async fn set_state(&mut self, next: ConnState) {
        if self.state == next {
            return;
        }

        let prev = self.state;
        self.state = next;

        self.bus
            .publish(AppEvent::Device(DeviceEvent::ConnStateChanged {
                device_id: self.device_id.clone(),
                from: prev,
                to: next,
            }));

        let mut snapshot = self
            .store
            .snapshot(&self.device_id)
            .await
            .unwrap_or_else(|| DeviceSnapshot {
                device_id: self.device_id.clone(),
                ..DeviceSnapshot::default()
            });
        snapshot.conn_state = next;
        snapshot.last_seen = Some(SystemTime::now());
        self.store.upsert_snapshot(snapshot.clone()).await;

        self.bus
            .publish(AppEvent::Device(DeviceEvent::TelemetryUpdated {
                device_id: self.device_id.clone(),
                snapshot,
            }));

        info!(device_id = %self.device_id, state = ?next, "session state changed");
    }
}

fn parse_sensor_value(payload: &[u8]) -> Option<(usize, f64)> {
    let s = std::str::from_utf8(payload).ok()?.trim();
    let mut sid = None;
    let mut value = None;
    for part in s.split(',') {
        let mut kv = part.splitn(2, '=');
        let k = kv.next()?.trim();
        let v = kv.next()?.trim();
        if k.eq_ignore_ascii_case("sid") {
            sid = v.parse::<usize>().ok();
        } else if k.eq_ignore_ascii_case("value") {
            value = v.parse::<f64>().ok();
        }
    }
    Some((sid?, value?))
}
