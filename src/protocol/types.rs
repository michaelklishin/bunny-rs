// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use bytes::Bytes;
use compact_str::CompactString;

use crate::errors::{NomError, ProtocolError};
use crate::protocol::constants::MAX_TABLE_DEPTH;

use nom::IResult;
use nom::bytes::complete::take;
use nom::number::complete::{
    be_f32, be_f64, be_i8, be_i16, be_i32, be_i64, be_u8, be_u16, be_u32, be_u64,
};

type NomResult<'a, T> = IResult<&'a [u8], T, NomError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal {
    pub scale: u8,
    pub value: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Bool(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    F32(f32),
    F64(f64),
    Decimal(Decimal),
    LongString(Bytes),
    Timestamp(u64),
    Table(FieldTable),
    Array(Vec<FieldValue>),
    ByteArray(Bytes),
    Void,
}

impl From<bool> for FieldValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}
impl From<i32> for FieldValue {
    fn from(v: i32) -> Self {
        Self::I32(v)
    }
}
impl From<i64> for FieldValue {
    fn from(v: i64) -> Self {
        Self::I64(v)
    }
}
impl From<f64> for FieldValue {
    fn from(v: f64) -> Self {
        Self::F64(v)
    }
}
impl From<&str> for FieldValue {
    fn from(v: &str) -> Self {
        Self::LongString(Bytes::copy_from_slice(v.as_bytes()))
    }
}
impl From<String> for FieldValue {
    fn from(v: String) -> Self {
        Self::LongString(Bytes::from(v))
    }
}
impl From<Bytes> for FieldValue {
    fn from(v: Bytes) -> Self {
        Self::LongString(v)
    }
}
impl From<FieldTable> for FieldValue {
    fn from(v: FieldTable) -> Self {
        Self::Table(v)
    }
}

/// Sorted key-value pairs (binary search lookup).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FieldTable(Vec<(CompactString, FieldValue)>);

impl FieldTable {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }

    pub fn insert(&mut self, key: impl Into<CompactString>, value: impl Into<FieldValue>) {
        let key = key.into();
        match self
            .0
            .binary_search_by(|(k, _)| k.as_str().cmp(key.as_str()))
        {
            Ok(idx) => self.0[idx].1 = value.into(),
            Err(idx) => self.0.insert(idx, (key, value.into())),
        }
    }

    pub fn get(&self, key: &str) -> Option<&FieldValue> {
        self.0
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|idx| &self.0[idx].1)
    }

    pub fn remove(&mut self, key: &str) -> Option<FieldValue> {
        self.0
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|idx| self.0.remove(idx).1)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &FieldValue)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn into_inner(self) -> Vec<(CompactString, FieldValue)> {
        self.0
    }
}

impl<K, V> FromIterator<(K, V)> for FieldTable
where
    K: Into<CompactString>,
    V: Into<FieldValue>,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let mut table = Self::new();
        for (k, v) in iter {
            table.insert(k, v);
        }
        table
    }
}

//
// Parsing
//

pub fn parse_short_string(input: &[u8]) -> NomResult<'_, &str> {
    let (input, len) = be_u8(input)?;
    let (input, data) = take(len as usize)(input)?;
    let s =
        simdutf8::basic::from_utf8(data).map_err(|_| ProtocolError::InvalidUtf8.nom_failure())?;
    Ok((input, s))
}

pub fn parse_long_string(input: &[u8]) -> NomResult<'_, &[u8]> {
    let (input, len) = be_u32(input)?;
    take(len as usize)(input)
}

pub fn parse_field_table(input: &[u8]) -> NomResult<'_, FieldTable> {
    parse_field_table_depth(input, 0)
}

fn parse_field_table_depth(input: &[u8], depth: u8) -> NomResult<'_, FieldTable> {
    if depth > MAX_TABLE_DEPTH {
        return Err(ProtocolError::TableNestingTooDeep {
            max: MAX_TABLE_DEPTH,
        }
        .nom_failure());
    }
    let (input, table_len) = be_u32(input)?;
    let (remaining, table_data) = take(table_len as usize)(input)?;

    let mut entries = Vec::new();
    let mut cursor = table_data;
    while !cursor.is_empty() {
        let (rest, key) = parse_short_string(cursor)?;
        let (rest, value) = parse_field_value_depth(rest, depth)?;
        entries.push((CompactString::new(key), value));
        cursor = rest;
    }
    entries.sort_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
    Ok((remaining, FieldTable(entries)))
}

pub fn parse_field_value(input: &[u8]) -> NomResult<'_, FieldValue> {
    parse_field_value_depth(input, 0)
}

fn parse_field_value_depth(input: &[u8], depth: u8) -> NomResult<'_, FieldValue> {
    let (input, tag) = be_u8(input)?;
    match tag {
        b't' => {
            let (i, v) = be_u8(input)?;
            Ok((i, FieldValue::Bool(v != 0)))
        }
        b'b' => {
            let (i, v) = be_i8(input)?;
            Ok((i, FieldValue::I8(v)))
        }
        b'B' => {
            let (i, v) = be_u8(input)?;
            Ok((i, FieldValue::U8(v)))
        }
        b's' => {
            let (i, v) = be_i16(input)?;
            Ok((i, FieldValue::I16(v)))
        }
        b'u' => {
            let (i, v) = be_u16(input)?;
            Ok((i, FieldValue::U16(v)))
        }
        b'I' => {
            let (i, v) = be_i32(input)?;
            Ok((i, FieldValue::I32(v)))
        }
        b'i' => {
            let (i, v) = be_u32(input)?;
            Ok((i, FieldValue::U32(v)))
        }
        b'l' => {
            let (i, v) = be_i64(input)?;
            Ok((i, FieldValue::I64(v)))
        }
        b'f' => {
            let (i, v) = be_f32(input)?;
            Ok((i, FieldValue::F32(v)))
        }
        b'd' => {
            let (i, v) = be_f64(input)?;
            Ok((i, FieldValue::F64(v)))
        }
        b'D' => {
            let (i, scale) = be_u8(input)?;
            let (i, value) = be_u32(i)?;
            Ok((i, FieldValue::Decimal(Decimal { scale, value })))
        }
        b'S' => {
            let (i, len) = be_u32(input)?;
            let (i, data) = take(len as usize)(i)?;
            Ok((i, FieldValue::LongString(Bytes::copy_from_slice(data))))
        }
        b'T' => {
            let (i, v) = be_u64(input)?;
            Ok((i, FieldValue::Timestamp(v)))
        }
        b'F' => {
            let (i, t) = parse_field_table_depth(input, depth + 1)?;
            Ok((i, FieldValue::Table(t)))
        }
        b'A' => parse_field_array_depth(input, depth + 1),
        b'x' => {
            let (i, len) = be_u32(input)?;
            let (i, data) = take(len as usize)(i)?;
            Ok((i, FieldValue::ByteArray(Bytes::copy_from_slice(data))))
        }
        b'V' => Ok((input, FieldValue::Void)),
        _ => Err(ProtocolError::UnknownFieldType(tag).nom_failure()),
    }
}

fn parse_field_array_depth(input: &[u8], depth: u8) -> NomResult<'_, FieldValue> {
    if depth > MAX_TABLE_DEPTH {
        return Err(ProtocolError::TableNestingTooDeep {
            max: MAX_TABLE_DEPTH,
        }
        .nom_failure());
    }
    let (input, array_len) = be_u32(input)?;
    let (remaining, array_data) = take(array_len as usize)(input)?;

    let mut items = Vec::new();
    let mut cursor = array_data;
    while !cursor.is_empty() {
        let (rest, value) = parse_field_value_depth(cursor, depth)?;
        items.push(value);
        cursor = rest;
    }
    Ok((remaining, FieldValue::Array(items)))
}

//
// Serialization
//

pub fn serialize_short_string(s: &str, buf: &mut Vec<u8>) -> Result<(), ProtocolError> {
    let len = s.len();
    if len > 255 {
        return Err(ProtocolError::ShortStringTooLong { len });
    }
    buf.push(len as u8);
    buf.extend_from_slice(s.as_bytes());
    Ok(())
}

pub fn serialize_long_string(data: &[u8], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
    buf.extend_from_slice(data);
}

pub fn serialize_field_table(table: &FieldTable, buf: &mut Vec<u8>) {
    let size_pos = buf.len();
    buf.extend_from_slice(&0u32.to_be_bytes()); // placeholder

    for (key, value) in table.iter() {
        buf.push(key.len() as u8);
        buf.extend_from_slice(key.as_bytes());
        serialize_field_value(value, buf);
    }

    let table_size = (buf.len() - size_pos - 4) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&table_size.to_be_bytes());
}

pub fn serialize_field_value(value: &FieldValue, buf: &mut Vec<u8>) {
    match value {
        FieldValue::Bool(v) => {
            buf.push(b't');
            buf.push(if *v { 1 } else { 0 });
        }
        FieldValue::I8(v) => {
            buf.push(b'b');
            buf.push(*v as u8);
        }
        FieldValue::U8(v) => {
            buf.push(b'B');
            buf.push(*v);
        }
        FieldValue::I16(v) => {
            buf.push(b's');
            buf.extend_from_slice(&v.to_be_bytes());
        }
        FieldValue::U16(v) => {
            buf.push(b'u');
            buf.extend_from_slice(&v.to_be_bytes());
        }
        FieldValue::I32(v) => {
            buf.push(b'I');
            buf.extend_from_slice(&v.to_be_bytes());
        }
        FieldValue::U32(v) => {
            buf.push(b'i');
            buf.extend_from_slice(&v.to_be_bytes());
        }
        FieldValue::I64(v) => {
            buf.push(b'l');
            buf.extend_from_slice(&v.to_be_bytes());
        }
        FieldValue::F32(v) => {
            buf.push(b'f');
            buf.extend_from_slice(&v.to_be_bytes());
        }
        FieldValue::F64(v) => {
            buf.push(b'd');
            buf.extend_from_slice(&v.to_be_bytes());
        }
        FieldValue::Decimal(d) => {
            buf.push(b'D');
            buf.push(d.scale);
            buf.extend_from_slice(&d.value.to_be_bytes());
        }
        FieldValue::LongString(v) => {
            buf.push(b'S');
            serialize_long_string(v, buf);
        }
        FieldValue::Timestamp(v) => {
            buf.push(b'T');
            buf.extend_from_slice(&v.to_be_bytes());
        }
        FieldValue::Table(t) => {
            buf.push(b'F');
            serialize_field_table(t, buf);
        }
        FieldValue::Array(arr) => {
            buf.push(b'A');
            let size_pos = buf.len();
            buf.extend_from_slice(&0u32.to_be_bytes());
            for item in arr {
                serialize_field_value(item, buf);
            }
            let arr_size = (buf.len() - size_pos - 4) as u32;
            buf[size_pos..size_pos + 4].copy_from_slice(&arr_size.to_be_bytes());
        }
        FieldValue::ByteArray(v) => {
            buf.push(b'x');
            buf.extend_from_slice(&(v.len() as u32).to_be_bytes());
            buf.extend_from_slice(v);
        }
        FieldValue::Void => {
            buf.push(b'V');
        }
    }
}
