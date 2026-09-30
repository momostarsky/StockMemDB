//! Fixed 48-byte frame header and protocol constants.

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// Magic prefix, ASCII "MSGX" stored little-endian (`0x5847534D`).
pub const MAGIC: u32 = u32::from_le_bytes(*b"MSGX");

/// Current wire protocol version.
pub const PROTOCOL_VERSION: u8 = 1;

/// Header size in bytes.
pub const HEADER_SIZE: usize = 48;

/// Defensive upper bound on a single frame body (16 MiB). Frames above this
/// are rejected as protocol errors rather than buffered.
pub const MAX_BODY_LEN: u32 = 16 * 1024 * 1024;

// The wire format is fixed little-endian; refuse to build anywhere else.
const _: () = assert!(cfg!(target_endian = "little"), "msg-proto requires little-endian");

/// Message types carried in [`FrameHeader::msg_type`].
#[allow(non_snake_case)]
pub mod MsgType {
    /// Request in REQ/REP mode (correlation_id required).
    pub const REQUEST: u16 = 1;
    /// Reply in REQ/REP mode (carries the request's correlation_id).
    pub const REPLY: u16 = 2;
    /// Application data in PUB/SUB mode.
    pub const PUBLISH: u16 = 3;
    /// Persistence / processing acknowledgement.
    pub const ACK: u16 = 4;
    /// Negative acknowledgement (retryable or terminal, see body error code).
    pub const NACK: u16 = 5;
    /// Connection / subscription control message.
    pub const CONTROL: u16 = 6;
}

/// Bit positions in [`FrameHeader::flags`].
#[allow(non_snake_case)]
pub mod Flags {
    /// This frame is a retry (same producer_id + seq as an earlier attempt).
    pub const RETRY: u8 = 1 << 0;
    /// Body is compressed (codec id is carried in the schema/body envelope).
    pub const COMPRESSED: u8 = 1 << 1;
    /// High priority delivery (jump the queue where the core allows it).
    pub const URGENT: u8 = 1 << 2;
}

/// Producer-assigned message identity: the dedupe key for idempotent ingest.
///
/// A retried frame carries the same `(producer_id, seq)` and sets
/// [`Flags::RETRY`]. The dedupe window itself lives in the core (M2); this
/// type fixes the key shape on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId {
    pub producer_id: u64,
    pub seq: u64,
}

impl MessageId {
    #[inline]
    pub const fn new(producer_id: u64, seq: u64) -> Self {
        Self { producer_id, seq }
    }
}

/// Fixed-size frame header. All fields are little-endian on the wire; the
/// packed layout is exactly 48 bytes with no padding and an alignment of 1,
/// so frames parse correctly at any buffer offset. Fields are only ever read
/// by copy (no references into the packed struct are taken).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned,
)]
#[repr(C, packed)]
pub struct FrameHeader {
    pub magic: u32,
    pub version: u8,
    pub flags: u8,
    pub msg_type: u16,
    pub topic_id: u32,
    pub schema_id: u16,
    /// Must be zero on the wire; rejected otherwise. Allocated to future use.
    pub reserved0: u16,
    pub body_len: u32,
    pub crc32c: u32,
    pub producer_id: u64,
    pub seq: u64,
    pub correlation_id: u64,
}

// Compile-time proof that the header is exactly the documented 48 bytes and
// contains no padding.
const _: () = assert!(core::mem::size_of::<FrameHeader>() == HEADER_SIZE);

impl FrameHeader {
    /// Construct a header for an outgoing frame. CRC is filled in by
    /// [`crate::encode_frame`]; `reserved0` is forced to zero.
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        msg_type: u16,
        topic_id: u32,
        schema_id: u16,
        flags: u8,
        producer_id: u64,
        seq: u64,
        correlation_id: u64,
        body_len: u32,
    ) -> Self {
        Self {
            magic: MAGIC,
            version: PROTOCOL_VERSION,
            flags,
            msg_type,
            topic_id,
            schema_id,
            reserved0: 0,
            body_len,
            crc32c: 0,
            producer_id,
            seq,
            correlation_id,
        }
    }

    /// Convenience for PUB/SUB frames (correlation_id = 0).
    #[inline]
    pub fn publish(
        topic_id: u32,
        schema_id: u16,
        producer_id: u64,
        seq: u64,
        body_len: u32,
    ) -> Self {
        Self::new(
            MsgType::PUBLISH,
            topic_id,
            schema_id,
            0,
            producer_id,
            seq,
            0,
            body_len,
        )
    }

    /// Convenience for REQ/REP requests.
    #[inline]
    pub fn request(
        topic_id: u32,
        schema_id: u16,
        producer_id: u64,
        seq: u64,
        correlation_id: u64,
        body_len: u32,
    ) -> Self {
        Self::new(
            MsgType::REQUEST,
            topic_id,
            schema_id,
            0,
            producer_id,
            seq,
            correlation_id,
            body_len,
        )
    }

    /// Convenience for REQ/REP replies.
    #[inline]
    pub fn reply(
        topic_id: u32,
        schema_id: u16,
        producer_id: u64,
        seq: u64,
        correlation_id: u64,
        body_len: u32,
    ) -> Self {
        Self::new(
            MsgType::REPLY,
            topic_id,
            schema_id,
            0,
            producer_id,
            seq,
            correlation_id,
            body_len,
        )
    }

    #[inline]
    pub fn message_id(&self) -> MessageId {
        MessageId::new(self.producer_id, self.seq)
    }

    #[inline]
    pub fn is_retry(&self) -> bool {
        self.flags & Flags::RETRY != 0
    }

    #[inline]
    pub fn total_len(&self) -> usize {
        HEADER_SIZE + self.body_len as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_size_is_48() {
        assert_eq!(core::mem::size_of::<FrameHeader>(), 48);
        assert_eq!(HEADER_SIZE, 48);
    }

    #[test]
    fn constructors_set_mode_fields() {
        let p = FrameHeader::publish(7, 9, 100, 1, 32);
        let (p_type, p_corr) = (p.msg_type, p.correlation_id);
        assert_eq!(p_type, MsgType::PUBLISH);
        assert_eq!(p_corr, 0);
        assert_eq!((p.topic_id, p.schema_id, p.body_len), (7, 9, 32));

        let q = FrameHeader::request(7, 9, 100, 2, 555, 0);
        let (q_type, q_corr) = (q.msg_type, q.correlation_id);
        assert_eq!(q_type, MsgType::REQUEST);
        assert_eq!(q_corr, 555);
    }
}
