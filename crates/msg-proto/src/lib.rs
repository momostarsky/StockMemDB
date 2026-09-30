//! # msg-proto
//!
//! Binary wire protocol v1 for brsk-msgx.
//!
//! Every message on the wire is a fixed 48-byte little-endian header followed
//! by a body (an SBE-encoded block in production; opaque here):
//!
//! ```text
//! offset  size  field
//! 0       4     magic ("MSGX")
//! 4       1     protocol version (currently 1)
//! 5       1     flags (retry / compressed / urgent)
//! 6       2     message type (MsgType)
//! 8       4     topic id
//! 12      2     schema id (SBE template/schema versioning)
//! 14      2     reserved (must be zero on the wire)
//! 16      4     body length in bytes
//! 20      4     CRC-32C over (header with crc zeroed) ++ body
//! 24      8     producer id
//! 32      8     producer sequence (dedupe key)
//! 40      8     correlation id (REQ/REP; 0 for PUB/SUB)
//! ```
//!
//! The header derives [`zerocopy::Unaligned`] so it can be parsed from any
//! offset in a TCP byte stream (the next frame's header is not guaranteed to
//! be 8-byte aligned). All multi-byte fields are little-endian on the wire.
//!
//! CRC-32C (Castagnoli) protects the whole frame; corrupted frames are
//! rejected before any field is trusted.

mod checksum;
mod frame;
mod header;

pub use frame::{decode_frame, encode_frame, encoded_len, peek_total_len, DecodedFrame};
pub use header::{
    Flags, FrameHeader, HEADER_SIZE, MAGIC, MAX_BODY_LEN, PROTOCOL_VERSION, MessageId, MsgType,
};

/// Protocol and frame errors. A peer that repeatedly sends bad frames must be
/// disconnected by the transport layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtoError {
    /// Fewer than [`HEADER_SIZE`] bytes available.
    TruncatedHeader,
    /// Whole frame not fully buffered yet (TCP framing); retry when more
    /// bytes arrive.
    TruncatedFrame { need: usize, have: usize },
    /// Magic prefix does not match; stream is out of sync or not our protocol.
    BadMagic(u32),
    /// Unsupported protocol version.
    BadVersion { supported: u8, got: u8 },
    /// Reserved header bytes were non-zero.
    NonZeroReserved,
    /// Declared body length exceeds [`MAX_BODY_LEN`].
    BodyTooLarge { len: u32, max: u32 },
    /// CRC-32C mismatch.
    CrcMismatch { expected: u32, actual: u32 },
}

impl core::fmt::Display for ProtoError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ProtoError::TruncatedHeader => write!(f, "not enough bytes for frame header"),
            ProtoError::TruncatedFrame { need, have } => write!(
                f,
                "frame truncated: need {need} bytes, have {have}"
            ),
            ProtoError::BadMagic(m) => write!(f, "bad magic: 0x{m:08X}"),
            ProtoError::BadVersion { supported, got } => write!(
                f,
                "unsupported protocol version: supported {supported}, got {got}"
            ),
            ProtoError::NonZeroReserved => write!(f, "reserved header bytes must be zero"),
            ProtoError::BodyTooLarge { len, max } => {
                write!(f, "body length {len} exceeds maximum {max}")
            }
            ProtoError::CrcMismatch { expected, actual } => write!(
                f,
                "crc-32c mismatch: expected 0x{expected:08X}, got 0x{actual:08X}"
            ),
        }
    }
}

impl std::error::Error for ProtoError {}
