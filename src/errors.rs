// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::error::Error as StdError;
use std::fmt;

pub type BoxError = Box<dyn StdError + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("frame too large: {size} bytes (max {max})")]
    FrameTooLarge { size: u32, max: u32 },

    #[error("invalid frame end byte: expected 0xCE, got 0x{0:02X}")]
    InvalidFrameEnd(u8),

    #[error("unknown frame type: {0}")]
    UnknownFrameType(u8),

    #[error("unknown method: class={class_id}, method={method_id}")]
    UnknownMethod { class_id: u16, method_id: u16 },

    #[error("unknown field type tag: 0x{0:02X}")]
    UnknownFieldType(u8),

    #[error("invalid UTF-8 in short string")]
    InvalidUtf8,

    #[error("field table nesting too deep (max {max})")]
    TableNestingTooDeep { max: u8 },

    #[error("trailing data after method: {bytes} extra bytes")]
    TrailingData { bytes: usize },

    #[error("version mismatch: server supports {server_major}.{server_minor}")]
    VersionMismatch { server_major: u8, server_minor: u8 },

    #[error("message body too large: {size} bytes (max {max})")]
    MessageTooLarge { size: u64, max: usize },

    #[error("short string too long: {len} bytes (max 255)")]
    ShortStringTooLong { len: usize },

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl ProtocolError {
    pub(crate) fn nom_failure(self) -> nom::Err<NomError> {
        nom::Err::Failure(NomError::Protocol(self))
    }
}

#[derive(Debug)]
pub enum NomError {
    Protocol(ProtocolError),
    Nom(nom::error::ErrorKind),
}

impl fmt::Display for NomError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NomError::Protocol(e) => write!(f, "{e}"),
            NomError::Nom(kind) => write!(f, "nom error: {kind:?}"),
        }
    }
}

impl nom::error::ParseError<&[u8]> for NomError {
    fn from_error_kind(_input: &[u8], kind: nom::error::ErrorKind) -> Self {
        NomError::Nom(kind)
    }

    fn append(_input: &[u8], _kind: nom::error::ErrorKind, other: Self) -> Self {
        other
    }
}
