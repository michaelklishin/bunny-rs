// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use bytes::Bytes;
use nom::IResult;
use nom::bytes::complete::take;
use nom::number::complete::{be_u8, be_u16, be_u32, be_u64};

use crate::errors::{NomError, ProtocolError};
use crate::protocol::constants::*;
use crate::protocol::method::{Method, parse_method, serialize_method};
use crate::protocol::properties::{BasicProperties, serialize_basic_properties};
use crate::protocol::types::serialize_short_string;

/// Pre-encode method + header + body frames into a single `Bytes` buffer.
pub fn serialize_publish_frames(
    channel_id: u16,
    method: &Method,
    properties_raw: &[u8],
    body: &[u8],
    max_body: usize,
) -> Result<Bytes, ProtocolError> {
    // Estimate: method ~64, header ~32 + props, body + frame overhead per chunk
    let estimate = 128 + properties_raw.len() + body.len() + FRAME_OVERHEAD * 3;
    let mut buf = Vec::with_capacity(estimate);

    // Method frame
    buf.push(FRAME_METHOD);
    buf.extend_from_slice(&channel_id.to_be_bytes());
    let size_pos = buf.len();
    buf.extend_from_slice(&0u32.to_be_bytes());
    serialize_method(method, &mut buf)?;
    let payload_size = (buf.len() - size_pos - 4) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&payload_size.to_be_bytes());
    buf.push(FRAME_END);

    // Header frame
    buf.push(FRAME_HEADER);
    buf.extend_from_slice(&channel_id.to_be_bytes());
    let size_pos = buf.len();
    buf.extend_from_slice(&0u32.to_be_bytes());
    buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes()); // weight
    buf.extend_from_slice(&(body.len() as u64).to_be_bytes());
    buf.extend_from_slice(properties_raw);
    let payload_size = (buf.len() - size_pos - 4) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&payload_size.to_be_bytes());
    buf.push(FRAME_END);

    // Body frame(s)
    if !body.is_empty() {
        let chunk_size = if max_body == 0 { body.len() } else { max_body };
        for chunk in body.chunks(chunk_size) {
            buf.push(FRAME_BODY);
            buf.extend_from_slice(&channel_id.to_be_bytes());
            buf.extend_from_slice(&(chunk.len() as u32).to_be_bytes());
            buf.extend_from_slice(chunk);
            buf.push(FRAME_END);
        }
    }

    Ok(Bytes::from(buf))
}

/// Encode a publish directly into the caller's buffer, bypassing Method enum
/// construction and intermediate allocations.
#[allow(clippy::too_many_arguments)]
pub fn encode_publish_direct(
    buf: &mut Vec<u8>,
    channel_id: u16,
    exchange: &str,
    routing_key: &str,
    mandatory: bool,
    properties: &BasicProperties,
    body: &[u8],
    max_body_per_frame: usize,
) -> Result<(), ProtocolError> {
    // Method frame: basic.publish
    buf.push(FRAME_METHOD);
    buf.extend_from_slice(&channel_id.to_be_bytes());
    let size_pos = buf.len();
    buf.extend_from_slice(&0u32.to_be_bytes());
    // class=60, method=40
    buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
    buf.extend_from_slice(&40u16.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes()); // ticket
    serialize_short_string(exchange, buf)?;
    serialize_short_string(routing_key, buf)?;
    buf.push(if mandatory { 1 } else { 0 });
    let payload_size = (buf.len() - size_pos - 4) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&payload_size.to_be_bytes());
    buf.push(FRAME_END);

    // Header frame: content header with properties written inline
    buf.push(FRAME_HEADER);
    buf.extend_from_slice(&channel_id.to_be_bytes());
    let size_pos = buf.len();
    buf.extend_from_slice(&0u32.to_be_bytes());
    buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes()); // weight
    buf.extend_from_slice(&(body.len() as u64).to_be_bytes());
    serialize_basic_properties(properties, buf)?;
    let payload_size = (buf.len() - size_pos - 4) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&payload_size.to_be_bytes());
    buf.push(FRAME_END);

    // Body frame(s)
    if !body.is_empty() {
        let chunk_size = if max_body_per_frame == 0 {
            body.len()
        } else {
            max_body_per_frame
        };
        for chunk in body.chunks(chunk_size) {
            buf.push(FRAME_BODY);
            buf.extend_from_slice(&channel_id.to_be_bytes());
            buf.extend_from_slice(&(chunk.len() as u32).to_be_bytes());
            buf.extend_from_slice(chunk);
            buf.push(FRAME_END);
        }
    }

    Ok(())
}

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
pub fn serialize_frame(frame: &Frame, buf: &mut Vec<u8>) -> Result<(), ProtocolError> {
    match frame {
        Frame::Method(channel_id, method) => {
            buf.push(FRAME_METHOD);
            buf.extend_from_slice(&channel_id.to_be_bytes());

            // Payload size placeholder
            let size_pos = buf.len();
            buf.extend_from_slice(&0u32.to_be_bytes());

            serialize_method(method, buf)?;

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
    Ok(())
}
