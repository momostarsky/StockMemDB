//! CRC-32C (Castagnoli) checksum.
//!
//! Implemented with the pure-Rust `crc` crate (CRC_32_ISCSI table) so the
//! workspace builds identically on Windows dev machines and Linux. A
//! SSE4.2/AVX hardware-accelerated implementation can be substituted behind
//! these functions without touching callers.

use crc::{CRC_32_ISCSI, Crc};

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Checksum of the exact bytes covered by a frame: header with its crc field
/// zeroed out, followed by the body.
#[inline]
pub fn frame_crc32c(header_bytes_with_zero_crc: &[u8], body: &[u8]) -> u32 {
    let mut digest = CRC32C.digest();
    digest.update(header_bytes_with_zero_crc);
    digest.update(body);
    digest.finalize()
}
