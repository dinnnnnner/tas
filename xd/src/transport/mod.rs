use async_trait::async_trait;
use bytes::BytesMut;

pub mod can;
use serialport::{DataBits, FlowControl, Parity, StopBits};
use std::io;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("not connected")]
    NotConnected,
    #[error("transport lock poisoned")]
    LockPoisoned,
}

#[async_trait]
pub trait Transport: Send + Sync {
    async fn connect(&mut self) -> Result<(), TransportError>;
    async fn read(&mut self, dst: &mut BytesMut) -> Result<usize, TransportError>;
    async fn write_all(&mut self, data: &[u8]) -> Result<(), TransportError>;
    async fn close(&mut self) -> Result<(), TransportError>;
}

pub struct TcpTransport {
    remote: SocketAddr,
    stream: Option<TcpStream>,
    read_chunk: usize,
}

pub struct ConnectedTcpTransport {
    stream: Option<TcpStream>,
    read_chunk: usize,
}

#[derive(Clone, Debug)]
pub struct SerialTransportConfig {
    pub port_name: String,
    pub baud_rate: u32,
    pub timeout: Duration,
    pub flow_control: FlowControl,
    pub data_bits: DataBits,
    pub parity: Parity,
    pub stop_bits: StopBits,
    pub dtr_enable: bool,
    pub rts_enable: bool,
    pub read_chunk: usize,
}

pub struct SerialTransport {
    config: SerialTransportConfig,
    port: Arc<Mutex<Option<Box<dyn serialport::SerialPort>>>>,
}

pub struct ConnectedSerialTransport {
    port: Arc<Mutex<Option<Box<dyn serialport::SerialPort>>>>,
    read_chunk: usize,
}

impl TcpTransport {
    pub fn new(remote: SocketAddr) -> Self {
        Self {
            remote,
            stream: None,
            read_chunk: 4096,
        }
    }

    fn stream_mut(&mut self) -> Result<&mut TcpStream, TransportError> {
        self.stream.as_mut().ok_or(TransportError::NotConnected)
    }
}

impl ConnectedTcpTransport {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: Some(stream),
            read_chunk: 4096,
        }
    }

    fn stream_mut(&mut self) -> Result<&mut TcpStream, TransportError> {
        self.stream.as_mut().ok_or(TransportError::NotConnected)
    }
}

impl Default for SerialTransportConfig {
    fn default() -> Self {
        Self {
            port_name: "COM1".to_string(),
            baud_rate: 115_200,
            timeout: Duration::from_millis(200),
            flow_control: FlowControl::None,
            data_bits: DataBits::Eight,
            parity: Parity::None,
            stop_bits: StopBits::One,
            dtr_enable: true,
            rts_enable: true,
            read_chunk: 4096,
        }
    }
}

impl SerialTransportConfig {
    pub fn new(port_name: impl Into<String>, baud_rate: u32) -> Self {
        Self {
            port_name: port_name.into(),
            baud_rate,
            ..Self::default()
        }
    }
}

impl SerialTransport {
    pub fn new(port_name: impl Into<String>, baud_rate: u32) -> Self {
        Self::with_config(SerialTransportConfig::new(port_name, baud_rate))
    }

    pub fn with_config(config: SerialTransportConfig) -> Self {
        Self {
            config,
            port: Arc::new(Mutex::new(None)),
        }
    }
}

impl ConnectedSerialTransport {
    pub fn from_port(port: Box<dyn serialport::SerialPort>, read_chunk: usize) -> Self {
        Self {
            port: Arc::new(Mutex::new(Some(port))),
            read_chunk,
        }
    }
}

fn join_to_io(err: tokio::task::JoinError) -> TransportError {
    TransportError::Io(io::Error::other(err.to_string()))
}

fn open_serial_port(config: &SerialTransportConfig) -> Result<Box<dyn serialport::SerialPort>, TransportError> {
    let mut port = serialport::new(&config.port_name, config.baud_rate)
        .timeout(config.timeout)
        .flow_control(config.flow_control)
        .data_bits(config.data_bits)
        .parity(config.parity)
        .stop_bits(config.stop_bits)
        .open()
        .map_err(|err| TransportError::Io(io::Error::other(err.to_string())))?;
    let _ = port.write_data_terminal_ready(config.dtr_enable);
    let _ = port.write_request_to_send(config.rts_enable);
    Ok(port)
}

async fn serial_read_into(
    port: Arc<Mutex<Option<Box<dyn serialport::SerialPort>>>>,
    read_chunk: usize,
    dst: &mut BytesMut,
) -> Result<usize, TransportError> {
    let (n, buf) = tokio::task::spawn_blocking(move || -> Result<(usize, Vec<u8>), TransportError> {
        let mut buf = vec![0u8; read_chunk];
        let mut guard = port.lock().map_err(|_| TransportError::LockPoisoned)?;
        let port = guard.as_mut().ok_or(TransportError::NotConnected)?;
        let n = port.read(&mut buf)?;
        Ok((n, buf))
    })
    .await
    .map_err(join_to_io)??;

    if n > 0 {
        dst.extend_from_slice(&buf[..n]);
    }
    Ok(n)
}

async fn serial_write_all(
    port: Arc<Mutex<Option<Box<dyn serialport::SerialPort>>>>,
    data: Vec<u8>,
) -> Result<(), TransportError> {
    tokio::task::spawn_blocking(move || -> Result<(), TransportError> {
        let mut guard = port.lock().map_err(|_| TransportError::LockPoisoned)?;
        let port = guard.as_mut().ok_or(TransportError::NotConnected)?;
        port.write_all(&data)?;
        port.flush()?;
        Ok(())
    })
    .await
    .map_err(join_to_io)??;
    Ok(())
}

async fn serial_close(port: Arc<Mutex<Option<Box<dyn serialport::SerialPort>>>>) -> Result<(), TransportError> {
    tokio::task::spawn_blocking(move || -> Result<(), TransportError> {
        let mut guard = port.lock().map_err(|_| TransportError::LockPoisoned)?;
        let _ = guard.take();
        Ok(())
    })
    .await
    .map_err(join_to_io)??;
    Ok(())
}

#[async_trait]
impl Transport for TcpTransport {
    async fn connect(&mut self) -> Result<(), TransportError> {
        let stream = TcpStream::connect(self.remote).await?;
        stream.set_nodelay(true)?;
        self.stream = Some(stream);
        Ok(())
    }

    async fn read(&mut self, dst: &mut BytesMut) -> Result<usize, TransportError> {
        let mut buf = vec![0u8; self.read_chunk];
        let n = self.stream_mut()?.read(&mut buf).await?;
        if n > 0 {
            dst.extend_from_slice(&buf[..n]);
        }
        Ok(n)
    }

    async fn write_all(&mut self, data: &[u8]) -> Result<(), TransportError> {
        self.stream_mut()?.write_all(data).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        if let Some(mut stream) = self.stream.take() {
            stream.shutdown().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Transport for ConnectedTcpTransport {
    async fn connect(&mut self) -> Result<(), TransportError> {
        if self.stream.is_some() {
            Ok(())
        } else {
            Err(TransportError::NotConnected)
        }
    }

    async fn read(&mut self, dst: &mut BytesMut) -> Result<usize, TransportError> {
        let mut buf = vec![0u8; self.read_chunk];
        let n = self.stream_mut()?.read(&mut buf).await?;
        if n > 0 {
            dst.extend_from_slice(&buf[..n]);
        }
        Ok(n)
    }

    async fn write_all(&mut self, data: &[u8]) -> Result<(), TransportError> {
        self.stream_mut()?.write_all(data).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        if let Some(mut stream) = self.stream.take() {
            stream.shutdown().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Transport for SerialTransport {
    async fn connect(&mut self) -> Result<(), TransportError> {
        let config = self.config.clone();
        let port = tokio::task::spawn_blocking(move || open_serial_port(&config))
            .await
            .map_err(join_to_io)??;
        let mut guard = self.port.lock().map_err(|_| TransportError::LockPoisoned)?;
        *guard = Some(port);
        Ok(())
    }

    async fn read(&mut self, dst: &mut BytesMut) -> Result<usize, TransportError> {
        serial_read_into(self.port.clone(), self.config.read_chunk, dst).await
    }

    async fn write_all(&mut self, data: &[u8]) -> Result<(), TransportError> {
        serial_write_all(self.port.clone(), data.to_vec()).await
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        serial_close(self.port.clone()).await
    }
}

#[async_trait]
impl Transport for ConnectedSerialTransport {
    async fn connect(&mut self) -> Result<(), TransportError> {
        let guard = self.port.lock().map_err(|_| TransportError::LockPoisoned)?;
        if guard.is_some() {
            Ok(())
        } else {
            Err(TransportError::NotConnected)
        }
    }

    async fn read(&mut self, dst: &mut BytesMut) -> Result<usize, TransportError> {
        serial_read_into(self.port.clone(), self.read_chunk, dst).await
    }

    async fn write_all(&mut self, data: &[u8]) -> Result<(), TransportError> {
        serial_write_all(self.port.clone(), data.to_vec()).await
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        serial_close(self.port.clone()).await
    }
}
