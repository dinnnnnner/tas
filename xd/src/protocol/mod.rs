use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;

pub mod demo_serial;

pub const MAGIC: u16 = 0xAA55;
pub const HEADER_LEN: usize = 2 + 2 + 8 + 1;
pub const CRC_LEN: usize = 2;
pub const SENT_SYNC_MARKER: u8 = 0xF0;
pub const SENT_FRAME_LEN: usize = 10;

#[derive(Clone, Debug)]
pub struct Frame {
    pub request_id: u64,
    pub kind: u8,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SentFrame {
    pub status: u8,
    pub channel_1: u16,
    pub channel_2: u16,
    pub crc: u8,
    pub pause: u8,
}

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("frame too large: {0}")]
    FrameTooLarge(usize),
    #[error("crc mismatch")]
    CrcMismatch,
    #[error("invalid length")]
    InvalidLength,
}

pub trait FrameCodec: Send + Sync {
    fn encode(&self, frame: &Frame, out: &mut BytesMut) -> Result<(), CodecError>;
    fn try_decode(&self, src: &mut BytesMut) -> Result<Option<Frame>, CodecError>;
}

#[derive(Default)]
pub struct SimpleFrameCodec {
    pub max_payload: usize,
}

#[derive(Default)]
pub struct SentFrameCodec;

impl SimpleFrameCodec {
    pub fn new(max_payload: usize) -> Self {
        Self { max_payload }
    }
}

impl SentFrameCodec {
    pub fn encode_frame(frame: SentFrame) -> [u8; SENT_FRAME_LEN] {
        [
            SENT_SYNC_MARKER,
            frame.status & 0x0F,
            ((frame.channel_1 >> 8) & 0x0F) as u8,
            ((frame.channel_1 >> 4) & 0x0F) as u8,
            (frame.channel_1 & 0x0F) as u8,
            (frame.channel_2 & 0x0F) as u8,
            ((frame.channel_2 >> 4) & 0x0F) as u8,
            ((frame.channel_2 >> 8) & 0x0F) as u8,
            frame.crc & 0x0F,
            frame.pause & 0x0F,
        ]
    }

    pub fn with_crc(status: u8, channel_1: u16, channel_2: u16, pause: u8) -> SentFrame {
        SentFrame {
            status: status & 0x0F,
            channel_1: channel_1 & 0x0FFF,
            channel_2: channel_2 & 0x0FFF,
            crc: sent_crc_nibble(status, channel_1, channel_2),
            pause: pause & 0x0F,
        }
    }

    pub fn sent3_with_crc(
        status: u8,
        secondary_angle: u16,
        rolling_counter: u8,
        pause: u8,
    ) -> SentFrame {
        let angle = secondary_angle & 0x0FFF;
        let counter = rolling_counter & 0x0F;
        let inverted_copy = sent3_inverted_copy(angle);
        let check_nibble = sent3_check_nibble(status, angle, counter, inverted_copy);
        let channel_2 = (counter as u16)
            | ((inverted_copy as u16) << 4)
            | ((check_nibble as u16) << 8);

        Self::with_crc(status, angle, channel_2, pause)
    }

    pub fn try_decode(&self, src: &mut BytesMut) -> Result<Option<SentFrame>, CodecError> {
        loop {
            let Some(sync_pos) = src.iter().position(|&b| b == SENT_SYNC_MARKER) else {
                src.clear();
                return Ok(None);
            };

            if sync_pos > 0 {
                src.advance(sync_pos);
            }

            if src.len() < SENT_FRAME_LEN {
                return Ok(None);
            }

            let raw = src.split_to(SENT_FRAME_LEN);
            let status = raw[1] & 0x0F;
            let channel_1 = (((raw[2] & 0x0F) as u16) << 8)
                | (((raw[3] & 0x0F) as u16) << 4)
                | (raw[4] & 0x0F) as u16;
            let channel_2 = (((raw[7] & 0x0F) as u16) << 8)
                | (((raw[6] & 0x0F) as u16) << 4)
                | (raw[5] & 0x0F) as u16;
            let crc = raw[8] & 0x0F;
            let pause = raw[9] & 0x0F;
            let expected_crc = sent_crc_nibble(status, channel_1, channel_2);

            if crc != expected_crc {
                continue;
            }

            return Ok(Some(SentFrame {
                status,
                channel_1,
                channel_2,
                crc,
                pause,
            }));
        }
    }
}

impl FrameCodec for SimpleFrameCodec {
    fn encode(&self, frame: &Frame, out: &mut BytesMut) -> Result<(), CodecError> {
        if frame.payload.len() > self.max_payload {
            return Err(CodecError::FrameTooLarge(frame.payload.len()));
        }

        let body_len = 8 + 1 + frame.payload.len();
        if body_len > u16::MAX as usize {
            return Err(CodecError::InvalidLength);
        }

        out.put_u16(MAGIC);
        out.put_u16(body_len as u16);
        out.put_u64(frame.request_id);
        out.put_u8(frame.kind);
        out.extend_from_slice(&frame.payload);

        let start = out.len() - body_len;
        let crc = crc16(&out[start..]);
        out.put_u16(crc);

        Ok(())
    }

    fn try_decode(&self, src: &mut BytesMut) -> Result<Option<Frame>, CodecError> {
        loop {
            if src.len() < HEADER_LEN + CRC_LEN {
                return Ok(None);
            }

            if src[0] != (MAGIC >> 8) as u8 || src[1] != (MAGIC & 0xFF) as u8 {
                src.advance(1);
                continue;
            }

            let body_len = u16::from_be_bytes([src[2], src[3]]) as usize;
            if body_len < 9 || body_len > self.max_payload + 9 {
                src.advance(1);
                continue;
            }

            let total_len = HEADER_LEN + (body_len - 9) + CRC_LEN;
            if src.len() < total_len {
                return Ok(None);
            }

            let frame_bytes = &src[4..(4 + body_len)];
            let recv_crc = u16::from_be_bytes([src[4 + body_len], src[4 + body_len + 1]]);
            let calc_crc = crc16(frame_bytes);
            if recv_crc != calc_crc {
                src.advance(1);
                continue;
            }

            let mut body = &src[4..(4 + body_len)];
            let request_id = body.get_u64();
            let kind = body.get_u8();
            let payload = body.to_vec();
            src.advance(total_len);

            return Ok(Some(Frame {
                request_id,
                kind,
                payload,
            }));
        }
    }
}

pub fn crc16(data: &[u8]) -> u16 {
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

pub fn sent_crc_nibble(status: u8, channel_1: u16, channel_2: u16) -> u8 {
    let nibbles = [
        status & 0x0F,
        ((channel_1 >> 8) & 0x0F) as u8,
        ((channel_1 >> 4) & 0x0F) as u8,
        (channel_1 & 0x0F) as u8,
        (channel_2 & 0x0F) as u8,
        ((channel_2 >> 4) & 0x0F) as u8,
        ((channel_2 >> 8) & 0x0F) as u8,
    ];

    let mut crc = 0x05_u8;
    for nibble in nibbles {
        crc ^= nibble & 0x0F;
        for _ in 0..4 {
            if (crc & 0x08) != 0 {
                crc = ((crc << 1) ^ 0x0D) & 0x0F;
            } else {
                crc = (crc << 1) & 0x0F;
            }
        }
    }
    crc & 0x0F
}

pub fn sent3_inverted_copy(secondary_angle: u16) -> u8 {
    (!((secondary_angle >> 8) as u8)) & 0x0F
}

pub fn sent3_check_nibble(
    status: u8,
    secondary_angle: u16,
    rolling_counter: u8,
    inverted_copy: u8,
) -> u8 {
    let angle_msn = ((secondary_angle >> 8) & 0x0F) as u8;
    let angle_mid = ((secondary_angle >> 4) & 0x0F) as u8;
    let angle_lsn = (secondary_angle & 0x0F) as u8;
    (status ^ angle_msn ^ angle_mid ^ angle_lsn ^ (rolling_counter & 0x0F) ^ (inverted_copy & 0x0F))
        & 0x0F
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_split_and_sticky_packets() {
        let codec = SimpleFrameCodec::new(1024);
        let mut buf = BytesMut::new();

        codec
            .encode(
                &Frame {
                    request_id: 1,
                    kind: 0x01,
                    payload: b"abc".to_vec(),
                },
                &mut buf,
            )
            .unwrap();
        codec
            .encode(
                &Frame {
                    request_id: 2,
                    kind: 0x02,
                    payload: b"xyz".to_vec(),
                },
                &mut buf,
            )
            .unwrap();

        let mut part = BytesMut::new();
        part.extend_from_slice(&buf[..5]);
        assert!(codec.try_decode(&mut part).unwrap().is_none());

        part.extend_from_slice(&buf[5..]);
        assert_eq!(codec.try_decode(&mut part).unwrap().unwrap().request_id, 1);
        assert_eq!(codec.try_decode(&mut part).unwrap().unwrap().request_id, 2);
        assert!(codec.try_decode(&mut part).unwrap().is_none());
    }

    #[test]
    fn sent_roundtrip() {
        let codec = SentFrameCodec;
        let frame = SentFrameCodec::with_crc(0x03, 0x08AB, 0x0734, 0x01);
        let raw = SentFrameCodec::encode_frame(frame);
        let mut buf = BytesMut::from(&raw[..]);
        let decoded = codec.try_decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, frame);
        assert!(buf.is_empty());
    }

    #[test]
    fn sent3_roundtrip() {
        let codec = SentFrameCodec;
        let frame = SentFrameCodec::sent3_with_crc(0x04, 0x08AB, 0x0C, 0x0B);
        let raw = SentFrameCodec::encode_frame(frame);
        let mut buf = BytesMut::from(&raw[..]);
        let decoded = codec.try_decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, frame);
        assert_eq!(decoded.channel_2 & 0x0F, 0x0C);
        assert_eq!((decoded.channel_2 >> 4) as u8 & 0x0F, sent3_inverted_copy(0x08AB));
        assert!(buf.is_empty());
    }
}
