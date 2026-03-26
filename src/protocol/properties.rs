// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use compact_str::CompactString;

use crate::errors::{NomError, ProtocolError};
use crate::protocol::types::{
    FieldTable, parse_field_table, parse_short_string, serialize_field_table,
    serialize_short_string,
};

use nom::IResult;
use nom::number::complete::{be_u8, be_u16, be_u64};

type NomResult<'a, T> = IResult<&'a [u8], T, NomError>;

// Property flag bits (MSB first)
const FLAG_CONTENT_TYPE: u16 = 0x8000;
const FLAG_CONTENT_ENCODING: u16 = 0x4000;
const FLAG_HEADERS: u16 = 0x2000;
const FLAG_DELIVERY_MODE: u16 = 0x1000;
const FLAG_PRIORITY: u16 = 0x0800;
const FLAG_CORRELATION_ID: u16 = 0x0400;
const FLAG_REPLY_TO: u16 = 0x0200;
const FLAG_EXPIRATION: u16 = 0x0100;
const FLAG_MESSAGE_ID: u16 = 0x0080;
const FLAG_TIMESTAMP: u16 = 0x0040;
const FLAG_TYPE: u16 = 0x0020;
const FLAG_USER_ID: u16 = 0x0010;
const FLAG_APP_ID: u16 = 0x0008;
const FLAG_CLUSTER_ID: u16 = 0x0004;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryMode {
    Transient = 1,
    Persistent = 2,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BasicProperties {
    pub(crate) content_type: Option<CompactString>,
    pub(crate) content_encoding: Option<CompactString>,
    pub(crate) headers: Option<FieldTable>,
    pub(crate) delivery_mode: Option<DeliveryMode>,
    pub(crate) priority: Option<u8>,
    pub(crate) correlation_id: Option<CompactString>,
    pub(crate) reply_to: Option<CompactString>,
    pub(crate) expiration: Option<CompactString>,
    pub(crate) message_id: Option<CompactString>,
    pub(crate) timestamp: Option<u64>,
    pub(crate) message_type: Option<CompactString>,
    pub(crate) user_id: Option<CompactString>,
    pub(crate) app_id: Option<CompactString>,
    pub(crate) cluster_id: Option<CompactString>,
}

// Builder methods
impl BasicProperties {
    pub fn content_type(mut self, v: impl Into<CompactString>) -> Self {
        self.content_type = Some(v.into());
        self
    }
    pub fn content_encoding(mut self, v: impl Into<CompactString>) -> Self {
        self.content_encoding = Some(v.into());
        self
    }
    pub fn headers(mut self, v: FieldTable) -> Self {
        self.headers = Some(v);
        self
    }
    pub fn delivery_mode(mut self, v: DeliveryMode) -> Self {
        self.delivery_mode = Some(v);
        self
    }
    pub fn persistent(mut self) -> Self {
        self.delivery_mode = Some(DeliveryMode::Persistent);
        self
    }
    pub fn transient(mut self) -> Self {
        self.delivery_mode = Some(DeliveryMode::Transient);
        self
    }
    pub fn priority(mut self, v: u8) -> Self {
        self.priority = Some(v);
        self
    }
    pub fn correlation_id(mut self, v: impl Into<CompactString>) -> Self {
        self.correlation_id = Some(v.into());
        self
    }
    pub fn reply_to(mut self, v: impl Into<CompactString>) -> Self {
        self.reply_to = Some(v.into());
        self
    }
    pub fn expiration(mut self, v: impl Into<CompactString>) -> Self {
        self.expiration = Some(v.into());
        self
    }
    pub fn message_id(mut self, v: impl Into<CompactString>) -> Self {
        self.message_id = Some(v.into());
        self
    }
    pub fn timestamp(mut self, v: u64) -> Self {
        self.timestamp = Some(v);
        self
    }
    pub fn message_type(mut self, v: impl Into<CompactString>) -> Self {
        self.message_type = Some(v.into());
        self
    }
    pub fn user_id(mut self, v: impl Into<CompactString>) -> Self {
        self.user_id = Some(v.into());
        self
    }
    pub fn app_id(mut self, v: impl Into<CompactString>) -> Self {
        self.app_id = Some(v.into());
        self
    }
}

// Getters
impl BasicProperties {
    pub fn get_content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }
    pub fn get_content_encoding(&self) -> Option<&str> {
        self.content_encoding.as_deref()
    }
    pub fn get_headers(&self) -> Option<&FieldTable> {
        self.headers.as_ref()
    }
    pub fn get_delivery_mode(&self) -> Option<DeliveryMode> {
        self.delivery_mode
    }
    pub fn get_priority(&self) -> Option<u8> {
        self.priority
    }
    pub fn get_correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }
    pub fn get_reply_to(&self) -> Option<&str> {
        self.reply_to.as_deref()
    }
    pub fn get_expiration(&self) -> Option<&str> {
        self.expiration.as_deref()
    }
    pub fn get_message_id(&self) -> Option<&str> {
        self.message_id.as_deref()
    }
    pub fn get_timestamp(&self) -> Option<u64> {
        self.timestamp
    }
    pub fn get_message_type(&self) -> Option<&str> {
        self.message_type.as_deref()
    }
    pub fn get_user_id(&self) -> Option<&str> {
        self.user_id.as_deref()
    }
    pub fn get_app_id(&self) -> Option<&str> {
        self.app_id.as_deref()
    }
    pub fn get_cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }
}

// Parsing
pub fn parse_basic_properties(input: &[u8]) -> NomResult<'_, BasicProperties> {
    let (mut input, flags) = be_u16(input)?;

    // Skip continuation flag words (bit 0). BasicProperties has 14 fields, fits in one word.
    if flags & 0x0001 != 0 {
        let mut cont = flags;
        while cont & 0x0001 != 0 {
            let (i, next) = be_u16(input)?;
            input = i;
            cont = next;
        }
    }

    let mut props = BasicProperties::default();

    macro_rules! parse_str {
        ($flag:expr, $field:ident) => {
            if flags & $flag != 0 {
                let (i, v) = parse_short_string(input)?;
                props.$field = Some(CompactString::new(v));
                input = i;
            }
        };
    }

    parse_str!(FLAG_CONTENT_TYPE, content_type);
    parse_str!(FLAG_CONTENT_ENCODING, content_encoding);

    if flags & FLAG_HEADERS != 0 {
        let (i, v) = parse_field_table(input)?;
        props.headers = Some(v);
        input = i;
    }
    if flags & FLAG_DELIVERY_MODE != 0 {
        let (i, v) = be_u8(input)?;
        props.delivery_mode = Some(if v == 2 {
            DeliveryMode::Persistent
        } else {
            DeliveryMode::Transient
        });
        input = i;
    }
    if flags & FLAG_PRIORITY != 0 {
        let (i, v) = be_u8(input)?;
        props.priority = Some(v);
        input = i;
    }

    parse_str!(FLAG_CORRELATION_ID, correlation_id);
    parse_str!(FLAG_REPLY_TO, reply_to);
    parse_str!(FLAG_EXPIRATION, expiration);
    parse_str!(FLAG_MESSAGE_ID, message_id);

    if flags & FLAG_TIMESTAMP != 0 {
        let (i, v) = be_u64(input)?;
        props.timestamp = Some(v);
        input = i;
    }

    parse_str!(FLAG_TYPE, message_type);
    parse_str!(FLAG_USER_ID, user_id);
    parse_str!(FLAG_APP_ID, app_id);
    parse_str!(FLAG_CLUSTER_ID, cluster_id);

    Ok((input, props))
}

// Serialization
pub fn serialize_basic_properties(
    props: &BasicProperties,
    buf: &mut Vec<u8>,
) -> Result<(), ProtocolError> {
    let mut flags: u16 = 0;

    if props.content_type.is_some() {
        flags |= FLAG_CONTENT_TYPE;
    }
    if props.content_encoding.is_some() {
        flags |= FLAG_CONTENT_ENCODING;
    }
    if props.headers.is_some() {
        flags |= FLAG_HEADERS;
    }
    if props.delivery_mode.is_some() {
        flags |= FLAG_DELIVERY_MODE;
    }
    if props.priority.is_some() {
        flags |= FLAG_PRIORITY;
    }
    if props.correlation_id.is_some() {
        flags |= FLAG_CORRELATION_ID;
    }
    if props.reply_to.is_some() {
        flags |= FLAG_REPLY_TO;
    }
    if props.expiration.is_some() {
        flags |= FLAG_EXPIRATION;
    }
    if props.message_id.is_some() {
        flags |= FLAG_MESSAGE_ID;
    }
    if props.timestamp.is_some() {
        flags |= FLAG_TIMESTAMP;
    }
    if props.message_type.is_some() {
        flags |= FLAG_TYPE;
    }
    if props.user_id.is_some() {
        flags |= FLAG_USER_ID;
    }
    if props.app_id.is_some() {
        flags |= FLAG_APP_ID;
    }
    if props.cluster_id.is_some() {
        flags |= FLAG_CLUSTER_ID;
    }

    buf.extend_from_slice(&flags.to_be_bytes());

    macro_rules! write_str {
        ($field:ident) => {
            if let Some(ref v) = props.$field {
                serialize_short_string(v, buf)?;
            }
        };
    }

    write_str!(content_type);
    write_str!(content_encoding);

    if let Some(ref h) = props.headers {
        serialize_field_table(h, buf);
    }
    if let Some(dm) = props.delivery_mode {
        buf.push(dm as u8);
    }
    if let Some(p) = props.priority {
        buf.push(p);
    }

    write_str!(correlation_id);
    write_str!(reply_to);
    write_str!(expiration);
    write_str!(message_id);

    if let Some(ts) = props.timestamp {
        buf.extend_from_slice(&ts.to_be_bytes());
    }

    write_str!(message_type);
    write_str!(user_id);
    write_str!(app_id);
    write_str!(cluster_id);

    Ok(())
}
