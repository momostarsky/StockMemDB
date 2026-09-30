//! Frame encoding, decoding and TCP-stream framing.

use zerocopy::IntoBytes;

use crate::checksum::frame_crc32c;
use crate::header::{FrameHeader, HEADER_SIZE, MAGIC, MAX_BODY_LEN, PROTOCOL_VERSION};
use crate::ProtoError;

/// A fully validated frame: owned header fields plus a borrow of the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedFrame<'a> {
    pub header: FrameHeader,
    pub body: &'a [u8],
}

/// Total wire length of a frame with `body_len` body bytes.
#[inline]
pub fn encoded_len(body_len: usize) -> usize {
    HEADER_SIZE + body_len
}

#[inline]
fn u16_at(b: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([b[off], b[off + 1]])
}

#[inline]
fn u32_at(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

#[inline]
fn u64_at(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().expect("8-byte slice"))
}

/// Validate header content shared by the framing probe and full decode.
/// `buf` must contain at least [`HEADER_SIZE`] bytes.
fn parse_header(buf: &[u8]) -> Result<FrameHeader, ProtoError> {
    let magic = u32_at(buf, 0);
    if magic != MAGIC {
        return Err(ProtoError::BadMagic(magic));
    }
    let version = buf[4];
    if version != PROTOCOL_VERSION {
        return Err(ProtoError::BadVersion {
            supported: PROTOCOL_VERSION,
            got: version,
        });
    }
    if u16_at(buf, 14) != 0 {
        return Err(ProtoError::NonZeroReserved);
    }
    let body_len = u32_at(buf, 16);
    if body_len > MAX_BODY_LEN {
        return Err(ProtoError::BodyTooLarge {
            len: body_len,
            max: MAX_BODY_LEN,
        });
    }

    Ok(FrameHeader {
        magic,
        version,
        flags: buf[5],
        msg_type: u16_at(buf, 6),
        topic_id: u32_at(buf, 8),
        schema_id: u16_at(buf, 12),
        reserved0: 0,
        body_len,
        crc32c: u32_at(buf, 20),
        producer_id: u64_at(buf, 24),
        seq: u64_at(buf, 32),
        correlation_id: u64_at(buf, 40),
    })
}

/// TCP-stream framing probe.
///
/// - `Ok(None)`: a complete frame is not buffered yet, wait for more bytes.
///   A bad magic / version is reported *as soon as the relevant bytes exist*,
///   i.e. potentially before the whole header arrives, so the transport can
///   resynchronize or disconnect instead of waiting forever.
/// - `Ok(Some(total))`: exactly one frame of `total` bytes sits at the start
///   of `buf`. The CRC is verified later by [`decode_frame`].
pub fn peek_total_len(buf: &[u8]) -> Result<Option<usize>, ProtoError> {
    // Magic as soon as 4 bytes exist.
    if buf.len() < 4 {
        return Ok(None);
    }
    let magic = u32_at(buf, 0);
    if magic != MAGIC {
        return Err(ProtoError::BadMagic(magic));
    }
    if buf.len() < 5 {
        return Ok(None);
    }
    let version = buf[4];
    if version != PROTOCOL_VERSION {
        return Err(ProtoError::BadVersion {
            supported: PROTOCOL_VERSION,
            got: version,
        });
    }
    // Need the body_len field at offset 16..20.
    if buf.len() < 20 {
        return Ok(None);
    }
    let body_len = u32_at(buf, 16);
    if body_len > MAX_BODY_LEN {
        return Err(ProtoError::BodyTooLarge {
            len: body_len,
            max: MAX_BODY_LEN,
        });
    }
    let total = encoded_len(body_len as usize);
    if buf.len() < total {
        Ok(None)
    } else {
        Ok(Some(total))
    }
}

/// Decode and fully validate one frame located at the start of `buf`.
///
/// Checks, in order: header presence, magic, version, reserved bytes, body
/// length cap, frame completeness, CRC-32C over header (crc field zeroed) and
/// body. Never trust a field on a frame that fails a later check.
pub fn decode_frame(buf: &[u8]) -> Result<DecodedFrame<'_>, ProtoError> {
    if buf.len() < HEADER_SIZE {
        return Err(ProtoError::TruncatedHeader);
    }
    let header = parse_header(buf)?;
    let total = header.total_len();
    if buf.len() < total {
        return Err(ProtoError::TruncatedFrame {
            need: total,
            have: buf.len(),
        });
    }

    // Recompute CRC with the on-wire crc field zeroed.
    let mut zeroed = [0u8; HEADER_SIZE];
    zeroed.copy_from_slice(&buf[..HEADER_SIZE]);
    zeroed[20..24].copy_from_slice(&0u32.to_le_bytes());
    let body = &buf[HEADER_SIZE..total];
    let actual = frame_crc32c(&zeroed, body);
    if actual != header.crc32c {
        return Err(ProtoError::CrcMismatch {
            expected: header.crc32c,
            actual,
        });
    }

    Ok(DecodedFrame { header, body })
}

/// Encode a frame: validates the header, fills the CRC, and returns the
/// complete bytes (header ++ body).
pub fn encode_frame(header: FrameHeader, body: &[u8]) -> Result<Vec<u8>, ProtoError> {
    if header.magic != MAGIC {
        return Err(ProtoError::BadMagic(header.magic));
    }
    if header.version != PROTOCOL_VERSION {
        return Err(ProtoError::BadVersion {
            supported: PROTOCOL_VERSION,
            got: header.version,
        });
    }
    if header.reserved0 != 0 {
        return Err(ProtoError::NonZeroReserved);
    }
    if body.len() as u64 > MAX_BODY_LEN as u64 {
        return Err(ProtoError::BodyTooLarge {
            len: body.len() as u32,
            max: MAX_BODY_LEN,
        });
    }
    if header.body_len as usize != body.len() {
        return Err(ProtoError::BodyTooLarge {
            len: body.len() as u32,
            max: MAX_BODY_LEN,
        });
    }

    let mut out = Vec::with_capacity(encoded_len(body.len()));
    let mut with_zero_crc = header;
    with_zero_crc.crc32c = 0;
    out.extend_from_slice(with_zero_crc.as_bytes());
    let crc = frame_crc32c(&out[..HEADER_SIZE], body);
    out[20..24].copy_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(body);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::{Flags, MessageId, MsgType};

    fn sample() -> (FrameHeader, Vec<u8>) {
        let body = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11];
        let h = FrameHeader::new(
            MsgType::REQUEST,
            42,
            9,
            Flags::URGENT,
            100_000,
            7,
            555,
            body.len() as u32,
        );
        (h, body)
    }

    #[test]
    fn round_trip_preserves_every_field() {
        let (h, body) = sample();
        let wire = encode_frame(h, &body).unwrap();
        assert_eq!(wire.len(), HEADER_SIZE + 6);

        let f = decode_frame(&wire).unwrap();
        assert_eq!(f.body, body);
        // CRC is filled by encode_frame (input header carries 0).
        let crc = f.header.crc32c;
        assert_ne!(crc, 0);
        let expected = FrameHeader {
            crc32c: crc,
            ..h
        };
        assert_eq!(f.header, expected);
        assert_eq!(f.header.message_id(), MessageId::new(100_000, 7));
        assert!(!f.header.is_retry());
        assert_ne!(f.header.flags & Flags::URGENT, 0);
    }

    #[test]
    fn empty_body_is_valid() {
        let h = FrameHeader::publish(1, 1, 2, 3, 0);
        let wire = encode_frame(h, &[]).unwrap();
        let f = decode_frame(&wire).unwrap();
        assert_eq!(f.body.len(), 0);
        assert_eq!(f.header.total_len(), HEADER_SIZE);
    }

    #[test]
    fn rejects_bad_magic_version_and_length() {
        let (h, body) = sample();
        let mut wire = encode_frame(h, &body).unwrap();

        wire[0] ^= 0xFF;
        assert!(matches!(decode_frame(&wire), Err(ProtoError::BadMagic(_))));
        assert!(matches!(peek_total_len(&wire), Err(ProtoError::BadMagic(_))));
        wire[0] ^= 0xFF; // restore

        wire[4] = 99;
        assert!(matches!(
            decode_frame(&wire),
            Err(ProtoError::BadVersion { got: 99, .. })
        ));
        wire[4] = PROTOCOL_VERSION;

        wire[14] = 1; // reserved
        assert!(matches!(decode_frame(&wire), Err(ProtoError::NonZeroReserved)));
        wire[14] = 0;

        // body_len beyond cap
        wire[16..20].copy_from_slice(&(MAX_BODY_LEN + 1).to_le_bytes());
        assert!(matches!(
            peek_total_len(&wire),
            Err(ProtoError::BodyTooLarge { .. })
        ));
    }

    #[test]
    fn rejects_crc_corruption() {
        let (h, body) = sample();
        let mut wire = encode_frame(h, &body).unwrap();

        // Flip a body byte.
        let last = wire.len() - 1;
        wire[last] ^= 0x01;
        assert!(matches!(decode_frame(&wire), Err(ProtoError::CrcMismatch { .. })));
        wire[last] ^= 0x01;

        // Flip a header byte (topic id). CRC mismatch must win over trusting
        // the field.
        wire[8] ^= 0x80;
        assert!(matches!(decode_frame(&wire), Err(ProtoError::CrcMismatch { .. })));
    }

    #[test]
    fn framing_probe_handles_partial_stream() {
        let (h, body) = sample();
        let wire = encode_frame(h, &body).unwrap();

        assert_eq!(peek_total_len(&[]).unwrap(), None);
        assert_eq!(peek_total_len(&wire[..3]).unwrap(), None);
        assert_eq!(peek_total_len(&wire[..19]).unwrap(), None);
        // Header complete, body missing.
        assert_eq!(peek_total_len(&wire[..HEADER_SIZE]).unwrap(), None);
        // One byte short of the full frame.
        assert_eq!(peek_total_len(&wire[..wire.len() - 1]).unwrap(), None);
        assert_eq!(
            peek_total_len(&wire).unwrap(),
            Some(HEADER_SIZE + body.len())
        );

        assert!(matches!(
            decode_frame(&wire[..HEADER_SIZE]),
            Err(ProtoError::TruncatedFrame { .. })
        ));
        assert!(matches!(
            decode_frame(&wire[..4]),
            Err(ProtoError::TruncatedHeader)
        ));
    }

    #[test]
    fn parses_from_unaligned_offset() {
        let (h, body) = sample();
        let wire = encode_frame(h, &body).unwrap();

        // Pad one byte so the frame starts at an odd address.
        let mut padded = vec![0xAAu8];
        padded.extend_from_slice(&wire);
        let f = decode_frame(&padded[1..]).unwrap();
        assert_eq!(f.body, body);
        let expected = FrameHeader {
            crc32c: f.header.crc32c,
            ..h
        };
        assert_eq!(f.header, expected);
    }

    #[test]
    fn encode_rejects_header_body_mismatch() {
        let (mut h, body) = sample();
        h.body_len = (body.len() + 1) as u32;
        assert!(encode_frame(h, &body).is_err());

        h.body_len = body.len() as u32;
        h.magic = 0;
        assert!(matches!(encode_frame(h, &body), Err(ProtoError::BadMagic(_))));
    }
}
