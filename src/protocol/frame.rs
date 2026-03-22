// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use bytes::Bytes;

use crate::errors::{NomError, ProtocolError};
use crate::protocol::constants::*;
use crate::protocol::method::{Method, parse_method, serialize_method};

use nom::IResult;
use nom::bytes::complete::take;
use nom::number::complete::{be_u8, be_u16, be_u32, be_u64};

type NomResult<'a, T> = IResult<&'a [u8], T, NomError>;

/// Parsed AMQP frame.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// Method frame.
    Method(u16, Box<Method>),
    /// Content header frame.
    Header(u16, ContentHeader),
    /// Content body frame.
    Body(u16, Bytes),
    /// Heartbeat (channel 0).
    Heartbeat,
}

/// Content header. Properties kept as raw bytes for lazy parsing.
#[derive(Debug, Clone, PartialEq)]
pub struct ContentHeader {
    pub class_id: u16,
    pub body_size: u64,
    pub properties_raw: Bytes,
}

/// Parse one frame.
///
/// Wire format: type(u8) channel(u16) size(u32) payload(size bytes) end(u8=0xCE)
pub fn parse_frame(input: &[u8]) -> NomResult<'_, Frame> {
    let (input, frame_type) = be_u8(input)?;
    let (input, channel_id) = be_u16(input)?;
    let (input, payload_size) = be_u32(input)?;
    let (input, payload) = take(payload_size as usize)(input)?;
    let (input, end_byte) = be_u8(input)?;

    if end_byte != FRAME_END {
        return Err(ProtocolError::InvalidFrameEnd(end_byte).nom_failure());
    }

    match frame_type {
        FRAME_METHOD => {
            let (rest, method) = parse_method(payload)?;
            if !rest.is_empty() {
                return Err(ProtocolError::TrailingData { bytes: rest.len() }.nom_failure());
            }
            Ok((input, Frame::Method(channel_id, Box::new(method))))
        }
        FRAME_HEADER => {
            let (payload, class_id) = be_u16(payload)?;
            let (payload, _weight) = be_u16(payload)?; // weight, always 0
            let (payload, body_size) = be_u64(payload)?;
            // Remaining payload bytes are the raw property flags + fields
            let properties_raw = Bytes::copy_from_slice(payload);
            Ok((
                input,
                Frame::Header(
                    channel_id,
                    ContentHeader {
                        class_id,
                        body_size,
                        properties_raw,
                    },
                ),
            ))
        }
        FRAME_BODY => {
            let body = Bytes::copy_from_slice(payload);
            Ok((input, Frame::Body(channel_id, body)))
        }
        FRAME_HEARTBEAT => Ok((input, Frame::Heartbeat)),
        _ => Err(ProtocolError::UnknownFrameType(frame_type).nom_failure()),
    }
}

/// Serialize a frame.
///
/// Wire format: type(u8) channel(u16) size(u32) payload(size bytes) end(u8=0xCE)
pub fn serialize_frame(frame: &Frame, buf: &mut Vec<u8>) {
    match frame {
        Frame::Method(channel_id, method) => {
            buf.push(FRAME_METHOD);
            buf.extend_from_slice(&channel_id.to_be_bytes());

            // Payload size placeholder
            let size_pos = buf.len();
            buf.extend_from_slice(&0u32.to_be_bytes());

            serialize_method(method, buf);

            // Patch payload size
            let payload_size = (buf.len() - size_pos - 4) as u32;
            buf[size_pos..size_pos + 4].copy_from_slice(&payload_size.to_be_bytes());

            buf.push(FRAME_END);
        }
        Frame::Header(channel_id, header) => {
            buf.push(FRAME_HEADER);
            buf.extend_from_slice(&channel_id.to_be_bytes());

            // Payload size placeholder
            let size_pos = buf.len();
            buf.extend_from_slice(&0u32.to_be_bytes());

            buf.extend_from_slice(&header.class_id.to_be_bytes());
            buf.extend_from_slice(&0u16.to_be_bytes()); // weight, always 0
            buf.extend_from_slice(&header.body_size.to_be_bytes());
            buf.extend_from_slice(&header.properties_raw);

            // Patch payload size
            let payload_size = (buf.len() - size_pos - 4) as u32;
            buf[size_pos..size_pos + 4].copy_from_slice(&payload_size.to_be_bytes());

            buf.push(FRAME_END);
        }
        Frame::Body(channel_id, body) => {
            buf.push(FRAME_BODY);
            buf.extend_from_slice(&channel_id.to_be_bytes());
            buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
            buf.extend_from_slice(body);
            buf.push(FRAME_END);
        }
        Frame::Heartbeat => {
            buf.push(FRAME_HEARTBEAT);
            buf.extend_from_slice(&0u16.to_be_bytes()); // channel 0
            buf.extend_from_slice(&0u32.to_be_bytes()); // no payload
            buf.push(FRAME_END);
        }
    }
}
