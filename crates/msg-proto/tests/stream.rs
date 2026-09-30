//! TCP stream behaviour: multiple frames in one buffer (coalescing), frames
//! split across reads, and a stream sync loss (bad magic) surfaced as an
//! error rather than an endless "need more bytes".

use msg_proto::{
    decode_frame, encode_frame, peek_total_len, Flags, FrameHeader, MsgType, ProtoError,
};

#[test]
fn multiple_frames_in_one_buffer() {
    let mut buf = Vec::new();
    let h1 = FrameHeader::publish(1, 1, 7, 1, 2);
    let h2 = FrameHeader::publish(2, 1, 7, 2, 3);
    buf.extend_from_slice(&encode_frame(h1, &[0xAA, 0xBB]).unwrap());
    buf.extend_from_slice(&encode_frame(h2, &[1, 2, 3]).unwrap());

    // Frame 1
    let len1 = peek_total_len(&buf).unwrap().unwrap();
    let f1 = decode_frame(&buf[..len1]).unwrap();
    assert_eq!(f1.body, &[0xAA, 0xBB]);
    let rest = &buf[len1..];

    // Frame 2 right behind it.
    let len2 = peek_total_len(rest).unwrap().unwrap();
    let f2 = decode_frame(&rest[..len2]).unwrap();
    assert_eq!(f2.body, &[1, 2, 3]);
    assert_eq!(len1 + len2, buf.len());
}

#[test]
fn frame_split_across_reads() {
    let h = FrameHeader::request(9, 1, 7, 1, 77, 4);
    let wire = encode_frame(h, &[1, 2, 3, 4]).unwrap();

    // Arriving one byte at a time: probe says "need more" until the last byte.
    for n in 0..wire.len() - 1 {
        assert_eq!(peek_total_len(&wire[..n]).unwrap(), None, "at {n}");
    }
    // Last byte completes it.
    assert_eq!(
        peek_total_len(&wire).unwrap(),
        Some(wire.len())
    );
    let f = decode_frame(&wire).unwrap();
    let (ty, corr) = (f.header.msg_type, f.header.correlation_id);
    assert_eq!(ty, MsgType::REQUEST);
    assert_eq!(corr, 77);
}

#[test]
fn desynced_stream_is_an_error_not_a_wait() {
    // A read whose first bytes are garbage (wrong protocol / lost sync).
    let garbage = [0u8, 1, 2, 3, 4, 5, 6, 7];
    assert!(matches!(
        peek_total_len(&garbage),
        Err(ProtoError::BadMagic(_))
    ));
}

#[test]
fn retry_flag_round_trips() {
    let h = FrameHeader::new(
        MsgType::PUBLISH,
        5,
        1,
        Flags::RETRY,
        42,
        99,
        0,
        0,
    );
    let wire = encode_frame(h, &[]).unwrap();
    let f = decode_frame(&wire).unwrap();
    assert!(f.header.is_retry());
    assert_eq!(f.header.message_id(), msg_proto::MessageId::new(42, 99));
}
