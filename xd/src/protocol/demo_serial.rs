use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

pub const HEADER: u8 = 0x7B;
pub const CMD_STREAM: u8 = 0x07;
pub const STREAM_TOTAL_LEN: usize = 0x2004;
pub const STREAM_BODY_LEN: usize = 8192;
pub const GROUP_SIZE: usize = 32;
pub const GROUP_COUNT: usize = 256;
pub const MIN_RESPONSE_LEN: usize = 7;
pub const MAX_FRAME_LEN: usize = u16::MAX as usize;
pub const MAX_REQUEST_PAYLOAD_LEN: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Command {
    UsbLinkCheck = 0x01,
    A2BNetNodesDiscovery = 0x02,
    GetNodeQuantity = 0x03,
    GetAllNodesUid = 0x04,
    ConfigureAdxl317 = 0x05,
    ReadAdxl317Config = 0x06,
    StartAccelerationAcquisition = 0x07,
    StopAccelerationAcquisition = 0x08,
    ResetSystem = 0x09,
    ReturnAccelerationData = 0x11,
    ReportSystemError = 0x20,
}

impl Command {
    pub fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            0x01 => Self::UsbLinkCheck,
            0x02 => Self::A2BNetNodesDiscovery,
            0x03 => Self::GetNodeQuantity,
            0x04 => Self::GetAllNodesUid,
            0x05 => Self::ConfigureAdxl317,
            0x06 => Self::ReadAdxl317Config,
            0x07 => Self::StartAccelerationAcquisition,
            0x08 => Self::StopAccelerationAcquisition,
            0x09 => Self::ResetSystem,
            0x11 => Self::ReturnAccelerationData,
            0x20 => Self::ReportSystemError,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandStatus {
    Ok = 0x01,
    FrameError = 0x02,
    FrameCrcErr = 0x03,
    UnsupportedCmd = 0x04,
    ParameterErr = 0x05,
    CommandExecuteFailed = 0x06,
    A2bNodeNotDiscovered = 0x07,
    A2bDiscoverNodeError = 0x08,
    A2bNodeDisconnected = 0x09,
    A2bNodeConfigError = 0x0A,
    A2bNodeReadConfigError = 0x0B,
    A2bNodeDataCollectError = 0x0C,
    A2bNodeStoppingError = 0x0D,
}

impl CommandStatus {
    pub fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            0x01 => Self::Ok,
            0x02 => Self::FrameError,
            0x03 => Self::FrameCrcErr,
            0x04 => Self::UnsupportedCmd,
            0x05 => Self::ParameterErr,
            0x06 => Self::CommandExecuteFailed,
            0x07 => Self::A2bNodeNotDiscovered,
            0x08 => Self::A2bDiscoverNodeError,
            0x09 => Self::A2bNodeDisconnected,
            0x0A => Self::A2bNodeConfigError,
            0x0B => Self::A2bNodeReadConfigError,
            0x0C => Self::A2bNodeDataCollectError,
            0x0D => Self::A2bNodeStoppingError,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RequestFrame {
    pub cmd: Command,
    pub payload: Vec<u8>,
}

impl RequestFrame {
    pub fn usb_link_check() -> Self {
        Self { cmd: Command::UsbLinkCheck, payload: Vec::new() }
    }

    pub fn a2b_net_nodes_discovery() -> Self {
        Self { cmd: Command::A2BNetNodesDiscovery, payload: Vec::new() }
    }

    pub fn get_node_quantity() -> Self {
        Self { cmd: Command::GetNodeQuantity, payload: Vec::new() }
    }

    pub fn get_all_nodes_uid() -> Self {
        Self { cmd: Command::GetAllNodesUid, payload: Vec::new() }
    }

    pub fn configure_adxl317(channel: u8, registers: &[u8]) -> Self {
        let mut payload = Vec::with_capacity(1 + registers.len());
        payload.push(channel);
        payload.extend_from_slice(registers);
        Self { cmd: Command::ConfigureAdxl317, payload }
    }

    pub fn read_adxl317_config(channel: u8) -> Self {
        Self { cmd: Command::ReadAdxl317Config, payload: vec![channel] }
    }

    pub fn start_acceleration_acquisition() -> Self {
        Self { cmd: Command::StartAccelerationAcquisition, payload: Vec::new() }
    }

    pub fn stop_acceleration_acquisition() -> Self {
        Self { cmd: Command::StopAccelerationAcquisition, payload: Vec::new() }
    }

    pub fn reset_system() -> Self {
        Self { cmd: Command::ResetSystem, payload: Vec::new() }
    }
}

#[derive(Debug, Error)]
pub enum SerialDemoCodecError {
    #[error("invalid frame length: {0}")]
    InvalidLength(usize),
    #[error("request payload too large: {0}")]
    RequestTooLarge(usize),
}

#[derive(Debug, Clone)]
pub enum Packet {
    Response(ResponseFrame),
    Stream(StreamFrame),
}

#[derive(Debug, Clone)]
pub struct ResponseFrame {
    pub cmd: u8,
    pub status: u8,
    pub payload: Bytes,
}

impl ResponseFrame {
    pub fn command(&self) -> Option<Command> {
        Command::from_u8(self.cmd)
    }

    pub fn command_status(&self) -> Option<CommandStatus> {
        CommandStatus::from_u8(self.status)
    }

    pub fn is_ok(&self) -> bool {
        self.command_status() == Some(CommandStatus::Ok)
    }
}

#[derive(Debug, Clone)]
pub struct StreamFrame {
    pub body: Bytes,
}

#[derive(Debug, Clone, Copy)]
pub struct Axis3 {
    pub x: i16,
    pub y: i16,
    pub z: i16,
    pub alarm: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct SampleGroup {
    pub ch4: Axis3,
    pub ch3: Axis3,
    pub ch2: Axis3,
    pub ch1: Axis3,
}

#[derive(Default)]
pub struct SerialDemoCodec;

impl SerialDemoCodec {
    pub fn encode_request(
        &self,
        request: &RequestFrame,
        dst: &mut BytesMut,
    ) -> Result<(), SerialDemoCodecError> {
        if request.payload.len() > MAX_REQUEST_PAYLOAD_LEN {
            return Err(SerialDemoCodecError::RequestTooLarge(request.payload.len()));
        }

        let frame_len = 1 + 2 + 1 + request.payload.len() + 2;
        if frame_len > MAX_FRAME_LEN {
            return Err(SerialDemoCodecError::InvalidLength(frame_len));
        }

        dst.reserve(frame_len);
        dst.put_u8(HEADER);
        dst.extend_from_slice(&(frame_len as u16).to_le_bytes());
        dst.put_u8(request.cmd as u8);
        dst.extend_from_slice(&request.payload);

        let start = dst.len() - (frame_len - 2);
        let crc = crc16_modbus(&dst[start..]);
        dst.extend_from_slice(&crc.to_le_bytes());
        Ok(())
    }

    pub fn build_request_bytes(
        &self,
        request: &RequestFrame,
    ) -> Result<Vec<u8>, SerialDemoCodecError> {
        let mut out = BytesMut::new();
        self.encode_request(request, &mut out)?;
        Ok(out.to_vec())
    }

    pub fn try_decode(&self, src: &mut BytesMut) -> Result<Option<Packet>, SerialDemoCodecError> {
        loop {
            let Some(pos) = src.iter().position(|&b| b == HEADER) else {
                src.clear();
                return Ok(None);
            };

            if pos > 0 {
                src.advance(pos);
            }

            if src.len() < 4 {
                return Ok(None);
            }

            let len = u16::from_le_bytes([src[1], src[2]]) as usize;
            let cmd = src[3];

            if cmd == CMD_STREAM && len == STREAM_TOTAL_LEN {
                if src.len() < STREAM_TOTAL_LEN {
                    return Ok(None);
                }

                let whole = src.split_to(STREAM_TOTAL_LEN).freeze();
                let body = whole.slice(4..);
                return Ok(Some(Packet::Stream(StreamFrame { body })));
            }

            if !(MIN_RESPONSE_LEN..=MAX_FRAME_LEN).contains(&len) {
                src.advance(1);
                continue;
            }

            if src.len() < len {
                return Ok(None);
            }

            let frame = &src[..len];
            let crc_recv = u16::from_le_bytes([frame[len - 2], frame[len - 1]]);
            let crc_calc = crc16_modbus(&frame[..len - 2]);
            if crc_recv != crc_calc {
                src.advance(1);
                continue;
            }

            let whole = src.split_to(len).freeze();
            let status = whole[4];
            let payload = whole.slice(5..len - 2);
            return Ok(Some(Packet::Response(ResponseFrame {
                cmd,
                status,
                payload,
            })));
        }
    }
}

pub fn crc16_modbus(data: &[u8]) -> u16 {
    let mut crc = 0xFFFFu16;
    for &b in data {
        crc ^= b as u16;
        for _ in 0..8 {
            if (crc & 0x0001) != 0 {
                crc = (crc >> 1) ^ 0xA001;
            } else {
                crc >>= 1;
            }
        }
    }
    crc
}

impl StreamFrame {
    pub fn groups(&self) -> impl Iterator<Item = SampleGroup> + '_ {
        self.body.chunks_exact(GROUP_SIZE).map(parse_group)
    }
}

fn parse_group(chunk: &[u8]) -> SampleGroup {
    SampleGroup {
        ch4: parse_channel(&chunk[0..8]),
        ch3: parse_channel(&chunk[8..16]),
        ch2: parse_channel(&chunk[16..24]),
        ch1: parse_channel(&chunk[24..32]),
    }
}

fn parse_channel(ch: &[u8]) -> Axis3 {
    Axis3 {
        x: raw_axis(&ch[0..2]),
        y: raw_axis(&ch[2..4]),
        z: raw_axis(&ch[4..6]),
        alarm: (ch[6] & 0x01) != 0,
    }
}

fn raw_axis(two: &[u8]) -> i16 {
    i16::from_le_bytes([two[0], two[1]]) >> 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_stream_frame_first() {
        let codec = SerialDemoCodec;
        let mut src = BytesMut::new();
        src.extend_from_slice(&[HEADER, 0x04, 0x20, CMD_STREAM]);
        src.resize(STREAM_TOTAL_LEN, 0);

        let packet = codec.try_decode(&mut src).unwrap().unwrap();
        match packet {
            Packet::Stream(frame) => {
                assert_eq!(frame.body.len(), STREAM_BODY_LEN);
                assert_eq!(frame.groups().count(), GROUP_COUNT);
            }
            Packet::Response(_) => panic!("expected stream packet"),
        }
    }

    #[test]
    fn encode_usb_link_check_request() {
        let codec = SerialDemoCodec;
        let bytes = codec
            .build_request_bytes(&RequestFrame::usb_link_check())
            .unwrap();
        assert_eq!(bytes[0], HEADER);
        assert_eq!(u16::from_le_bytes([bytes[1], bytes[2]]), 6);
        assert_eq!(bytes[3], Command::UsbLinkCheck as u8);
    }
}
