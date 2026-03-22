// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use bytes::{Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::errors::ProtocolError;
use crate::protocol::constants::*;
use crate::protocol::frame::{ContentHeader, Frame, serialize_frame};

enum CodecState {
    AwaitingProtocolHeader,
    Framing,
}

pub struct AmqpCodec {
    state: CodecState,
    max_frame_size: u32,
}

impl AmqpCodec {
    pub fn new(max_frame_size: u32) -> Self {
        Self {
            state: CodecState::AwaitingProtocolHeader,
            max_frame_size,
        }
    }

    pub fn new_framing(max_frame_size: u32) -> Self {
        Self {
            state: CodecState::Framing,
            max_frame_size,
        }
    }
}

impl Decoder for AmqpCodec {
    type Item = Frame;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, ProtocolError> {
        match self.state {
            CodecState::AwaitingProtocolHeader => {
                if src.is_empty() {
                    return Ok(None);
                }
                // 'A' = version mismatch (server sends back a protocol header)
                if src[0] == b'A' {
                    if src.len() < 8 {
                        return Ok(None);
                    }
                    let header = src.split_to(8);
                    return Err(ProtocolError::VersionMismatch {
                        server_major: header[6],
                        server_minor: header[7],
                    });
                }
                self.state = CodecState::Framing;
                self.decode(src)
            }
            CodecState::Framing => {
                if src.len() < 7 {
                    return Ok(None);
                }

                let raw_payload_size = u32::from_be_bytes([src[3], src[4], src[5], src[6]]);

                if raw_payload_size > self.max_frame_size {
                    return Err(ProtocolError::FrameTooLarge {
                        size: raw_payload_size,
                        max: self.max_frame_size,
                    });
                }

                let payload_size = raw_payload_size as usize;
                let total = 7usize
                    .checked_add(payload_size)
                    .and_then(|n| n.checked_add(1))
                    .ok_or(ProtocolError::FrameTooLarge {
                        size: raw_payload_size,
                        max: self.max_frame_size,
                    })?;

                if src.len() < total {
                    src.reserve(total - src.len());
                    return Ok(None);
                }

                // Freeze for zero-copy body frames
                let frozen: Bytes = src.split_to(total).freeze();
                let raw = frozen.as_ref();

                let frame_type = raw[0];
                let channel_id = u16::from_be_bytes([raw[1], raw[2]]);
                let end_byte = raw[7 + payload_size];

                if end_byte != FRAME_END {
                    return Err(ProtocolError::InvalidFrameEnd(end_byte));
                }

                match frame_type {
                    FRAME_BODY => {
                        // Zero-copy slice
                        let body = frozen.slice(7..7 + payload_size);
                        Ok(Some(Frame::Body(channel_id, body)))
                    }
                    FRAME_HEARTBEAT => Ok(Some(Frame::Heartbeat)),
                    FRAME_METHOD | FRAME_HEADER => {
                        let payload = &raw[7..7 + payload_size];
                        match frame_type {
                            FRAME_METHOD => {
                                let (rest, method) = crate::protocol::method::parse_method(payload)
                                    .map_err(|e| ProtocolError::Parse(format!("{e:?}")))?;
                                if !rest.is_empty() {
                                    return Err(ProtocolError::TrailingData { bytes: rest.len() });
                                }
                                Ok(Some(Frame::Method(channel_id, Box::new(method))))
                            }
                            FRAME_HEADER => {
                                let (payload, class_id) = nom::number::complete::be_u16::<
                                    _,
                                    nom::error::Error<&[u8]>,
                                >(payload)
                                .map_err(|e| ProtocolError::Parse(format!("{e:?}")))?;
                                let (payload, _weight) = nom::number::complete::be_u16::<
                                    _,
                                    nom::error::Error<&[u8]>,
                                >(payload)
                                .map_err(|e| ProtocolError::Parse(format!("{e:?}")))?;
                                let (_payload, body_size) =
                                    nom::number::complete::be_u64::<_, nom::error::Error<&[u8]>>(
                                        payload,
                                    )
                                    .map_err(|e| ProtocolError::Parse(format!("{e:?}")))?;

                                // Zero-copy properties
                                let props_offset = 7 + 2 + 2 + 8; // frame header parsed before + class_id + weight + body_size
                                let props_end = 7 + payload_size;
                                let properties_raw = if props_offset < props_end {
                                    frozen.slice(props_offset..props_end)
                                } else {
                                    Bytes::new()
                                };

                                Ok(Some(Frame::Header(
                                    channel_id,
                                    ContentHeader {
                                        class_id,
                                        body_size,
                                        properties_raw,
                                    },
                                )))
                            }
                            _ => Err(ProtocolError::UnknownFrameType(frame_type)),
                        }
                    }
                    _ => Err(ProtocolError::UnknownFrameType(frame_type)),
                }
            }
        }
    }
}

impl Encoder<Frame> for AmqpCodec {
    type Error = ProtocolError;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), ProtocolError> {
        let mut buf = Vec::with_capacity(128);
        serialize_frame(&frame, &mut buf);
        dst.extend_from_slice(&buf);
        Ok(())
    }
}
