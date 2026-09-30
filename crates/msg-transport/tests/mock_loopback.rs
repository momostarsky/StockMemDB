//! MockTransport contract tests using real msg-proto wire frames.

use std::time::Duration;

use crossbeam_channel::TryRecvError;
use msg_proto::{decode_frame, encode_frame, FrameHeader, MsgType};
use msg_transport::{connected_pair, TransportCaps, TransportError, TransportEvent};
use msg_transport::{Transport, TransportInfo};

fn wire_frame(seq: u64, body: &[u8]) -> Vec<u8> {
    let h = FrameHeader::publish(42, 1, 7, seq, body.len() as u32);
    encode_frame(h, body).unwrap()
}

fn wait_until<F: FnMut() -> bool>(mut f: F) {
    for _ in 0..400 {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("condition not reached within timeout");
}

fn drain_events(rx: &crossbeam_channel::Receiver<TransportEvent>) -> Vec<TransportEvent> {
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    out
}

#[test]
fn bidirectional_loopback_of_wire_frames() {
    let (a, b) = connected_pair(16);
    let a = a.start();
    let b = b.start();

    // A -> B
    let req = wire_frame(1, &[1, 2, 3, 4]);
    a.endpoint.send_bytes(&req).unwrap();
    wait_until(|| b.endpoint.inbound_len() >= 1);
    let inb = b.endpoint.try_recv().unwrap();
    assert!(inb.meta.recv_ts_us > 0);
    assert!(!inb.meta.hardware_ts);
    let decoded = decode_frame(inb.frame.payload()).unwrap();
    let seq = decoded.header.seq; // packed struct: copy fields out
    assert_eq!(seq, 1);
    assert_eq!(decoded.body, &[1, 2, 3, 4]);
    inb.frame.release();

    // B -> A reply
    let reply_h = FrameHeader::reply(42, 1, 7, 2, 555, 2);
    let reply = encode_frame(reply_h, &[9, 9]).unwrap();
    b.endpoint.send_bytes(&reply).unwrap();
    wait_until(|| a.endpoint.inbound_len() >= 1);
    let inb = a.endpoint.try_recv().unwrap();
    let decoded = decode_frame(inb.frame.payload()).unwrap();
    let msg_type = decoded.header.msg_type;
    let correlation_id = decoded.header.correlation_id;
    assert_eq!(msg_type, MsgType::REPLY);
    assert_eq!(correlation_id, 555);
    inb.frame.release();

    a.shutdown();
    b.shutdown();
}

#[test]
fn batch_drain() {
    let (a, b) = connected_pair(64);
    let a = a.start();
    let b = b.start();

    for seq in 0..20u64 {
        a.endpoint.send_bytes(&wire_frame(seq, &[seq as u8])).unwrap();
    }
    wait_until(|| b.endpoint.inbound_len() == 20);

    let mut batch = Vec::new();
    let n = b.endpoint.recv_batch(&mut batch, 20);
    assert_eq!(n, 20);
    for (i, item) in batch.iter().enumerate() {
        assert_eq!(item.frame.len(), 49); // 48-byte header + 1 body byte
        let d = decode_frame(item.frame.payload()).unwrap();
        let seq = d.header.seq;
        assert_eq!(seq, i as u64);
    }
    for f in batch {
        f.frame.release();
    }
    assert_eq!(b.endpoint.try_recv().unwrap_err(), TryRecvError::Empty);

    a.shutdown();
    b.shutdown();
}

#[test]
fn outbound_queue_full_returns_frame() {
    // A's pump is NOT started: its outbound queue never drains, so bounded
    // capacity is enforced deterministically at the core side.
    let (a, _b) = connected_pair(2);
    let ep = a.endpoint();

    ep.send_bytes(&wire_frame(1, &[1])).unwrap();
    ep.send_bytes(&wire_frame(2, &[2])).unwrap();
    let err = ep.send_bytes(&wire_frame(3, &[3])).unwrap_err();
    match err {
        TransportError::QueueFull(returned) => {
            // The unsent frame is handed back intact (sized 49) for retry.
            assert_eq!(returned.len(), 49);
        }
        other => panic!("expected queue full, got {other}"),
    }
    let snap = ep.metrics().snapshot();
    assert_eq!(snap.tx_full, 1);
    assert_eq!(snap.tx_frames, 2);
}

#[test]
fn inbound_overflow_drops_counts_and_emits_event() {
    // Start only A's pump. B's inbound ring (cap 2) fills and overflows
    // deterministically because nobody drains B.
    let (a, b) = connected_pair(2);
    let b_ep = b.endpoint();
    let b_metrics = b.metrics();
    let a = a.start();
    // side b deliberately not started (endpoint keeps the receiver alive)

    // Push well past both ring capacities; core-side QueueFull results are
    // ignored on purpose here. The B inbound ring can only ever accept 2.
    for seq in 0..200u64 {
        if a.endpoint.send_bytes(&wire_frame(seq, &[0])).is_err() {
            std::thread::sleep(Duration::from_micros(200));
        }
    }

    wait_until(|| b_metrics.snapshot().rx_dropped >= 4);
    let snap = b_metrics.snapshot();
    assert_eq!(snap.rx_frames, 2);
    assert!(snap.rx_dropped >= 4);

    // The sender-side pump emitted overflow events for the peer's ring.
    wait_until(|| {
        drain_events(&a.events)
            .iter()
            .any(|e| matches!(e, TransportEvent::InboundOverflow { .. }))
    });

    // The two accepted frames remain intact in the ring.
    let f1 = b_ep.try_recv().unwrap();
    let f2 = b_ep.try_recv().unwrap();
    decode_frame(f1.frame.payload()).unwrap();
    decode_frame(f2.frame.payload()).unwrap();
    f1.frame.release();
    f2.frame.release();

    a.shutdown();
}

#[test]
fn implicit_release_is_counted() {
    let (a, b) = connected_pair(4);
    let b_metrics = b.metrics();
    let a = a.start();
    let b = b.start();

    a.endpoint.send_bytes(&wire_frame(1, &[7])).unwrap();
    wait_until(|| b.endpoint.inbound_len() >= 1);
    {
        let inb = b.endpoint.try_recv().unwrap();
        decode_frame(inb.frame.payload()).unwrap();
        // Dropped without explicit release().
    }
    wait_until(|| b_metrics.snapshot().rx_implicit_release == 1);
    assert_eq!(b_metrics.snapshot().rx_implicit_release, 1);

    a.shutdown();
    b.shutdown();
}

#[test]
fn info_caps_and_lifecycle_events() {
    let (a, b) = connected_pair(4);
    let a_transport = a.transport();
    let info: &TransportInfo = a_transport.info();
    assert_eq!(info.name, "mock-a");
    assert!(info.has(TransportCaps::IN_PROCESS));
    assert!(!info.has(TransportCaps::ZERO_COPY_RX));
    assert!(a.transport().info().metrics.snapshot().fatal_errors == 0);

    let a = a.start();
    let b = b.start();
    wait_until(|| drain_events(&a.events).iter().any(|e| matches!(e, TransportEvent::Started { .. })));
    wait_until(|| drain_events(&b.events).iter().any(|e| matches!(e, TransportEvent::Started { .. })));

    let a_events_rx = a.shutdown();
    let b_events_rx = b.shutdown();
    wait_until(|| drain_events(&a_events_rx).iter().any(|e| matches!(e, TransportEvent::Stopped { .. })));
    wait_until(|| drain_events(&b_events_rx).iter().any(|e| matches!(e, TransportEvent::Stopped { .. })));
}
