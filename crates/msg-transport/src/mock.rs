//! In-process mock transport: a connected pair of transports that relay
//! complete frames through real bounded channels and a pump thread.
//!
//! It exercises the entire contract (batching, explicit backpressure on
//! send, inbound overflow drop + event, timestamps and stop) without Linux,
//! sockets or NICs, so the message core and its tests run on Windows/CI.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{Receiver, TrySendError};

use crate::frame::{CoreEndpoint, Inbound, Outbound, RxFrame, RxMeta, TransportCtx};
use crate::metrics::TransportMetrics;
use crate::transport::{
    StopToken, Transport, TransportCaps, TransportEvent, TransportHandle, TransportInfo,
};

/// How often a pump thread wakes to observe the stop flag while idle.
const PUMP_IDLE_TICK: Duration = Duration::from_millis(10);
const EVENT_QUEUE_CAP: usize = 256;

/// Link to the peer side, injected during pair construction.
struct PeerLink {
    name: &'static str,
    inbound: crossbeam_channel::Sender<Inbound>,
    metrics: Arc<TransportMetrics>,
}

/// In-process loopback transport. Obtain via [`connected_pair`].
pub struct MockTransport {
    info: TransportInfo,
    peer: Mutex<Option<PeerLink>>,
}

impl MockTransport {
    fn new(name: &'static str) -> Arc<Self> {
        Arc::new(Self {
            info: TransportInfo {
                name,
                caps: TransportCaps::IN_PROCESS,
                metrics: Arc::new(TransportMetrics::new()),
            },
            peer: Mutex::new(None),
        })
    }

    fn attach_peer(&self, peer: PeerLink) {
        *self.peer.lock().expect("mock peer mutex") = Some(peer);
    }
}

impl Transport for MockTransport {
    fn info(&self) -> &TransportInfo {
        &self.info
    }

    fn start(self: Arc<Self>, ctx: TransportCtx) -> TransportHandle {
        let stop = StopToken::new();
        let handle = TransportHandle::new(self.info.name, stop.clone());
        let builder =
            std::thread::Builder::new().name(format!("mock-pump-{}", self.info.name));
        let join = builder
            .spawn(move || self.run(ctx, stop))
            .expect("spawn mock pump");
        handle.add_thread(join);
        handle
    }
}

impl MockTransport {
    fn run(self: Arc<Self>, ctx: TransportCtx, stop: StopToken) {
        let name = self.info.name;
        let _ = ctx.events.send(TransportEvent::Started {
            name: name.to_string(),
        });

        while !stop.is_stopped() {
            match ctx.outbound.recv_timeout(PUMP_IDLE_TICK) {
                Ok(Outbound { frame }) => {
                    let bytes = frame.into_bytes();
                    let n = bytes.len() as u64;
                    let peer = self
                        .peer
                        .lock()
                        .expect("mock peer mutex")
                        .as_ref()
                        .map(|p| PeerLink {
                            name: p.name,
                            inbound: p.inbound.clone(),
                            metrics: p.metrics.clone(),
                        });
                    let Some(peer) = peer else {
                        self.info.metrics.fatal_errors.fetch_add(1, Ordering::Relaxed);
                        let _ = ctx.events.try_send(TransportEvent::Fatal {
                            name: name.to_string(),
                            error: "mock transport started without a peer".to_string(),
                        });
                        break;
                    };

                    let inbound = Inbound {
                        frame: RxFrame::heap(bytes, peer.metrics.clone()),
                        meta: RxMeta::software_now(),
                    };
                    match peer.inbound.try_send(inbound) {
                        Ok(()) => {
                            peer.metrics.rx_frames.fetch_add(1, Ordering::Relaxed);
                            peer.metrics.rx_bytes.fetch_add(n, Ordering::Relaxed);
                        }
                        Err(TrySendError::Full(item)) => {
                            // Mirror AF_XDP ring semantics: drop, count, alert.
                            let dropped =
                                peer.metrics.rx_dropped.fetch_add(1, Ordering::Relaxed) + 1;
                            self.emit_event(
                                &ctx,
                                TransportEvent::InboundOverflow {
                                    name: peer.name.to_string(),
                                    dropped_total: dropped,
                                },
                            );
                            drop(item);
                        }
                        Err(TrySendError::Disconnected(_)) => break,
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }

        let _ = ctx.events.send(TransportEvent::Stopped {
            name: name.to_string(),
        });
    }

    fn emit_event(&self, ctx: &TransportCtx, event: TransportEvent) {
        if ctx.events.try_send(event).is_err() {
            self.info
                .metrics
                .events_dropped
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// One side of a connected mock pair: core endpoint, event stream and the
/// not-yet-started transport.
pub struct MockSide {
    pub endpoint: CoreEndpoint,
    events_rx: Receiver<TransportEvent>,
    transport: Arc<MockTransport>,
    ctx: Option<TransportCtx>,
}

impl MockSide {
    pub fn transport(&self) -> Arc<MockTransport> {
        self.transport.clone()
    }

    pub fn metrics(&self) -> Arc<TransportMetrics> {
        self.transport.info().metrics.clone()
    }

    /// Core endpoint handle (cheap to clone; usable before or without start).
    pub fn endpoint(&self) -> CoreEndpoint {
        self.endpoint.clone()
    }

    /// Spawn the pump thread and start relaying.
    pub fn start(mut self) -> Started {
        let ctx = self.ctx.take().expect("mock side already started");
        let handle = self.transport.clone().start(ctx);
        Started {
            endpoint: self.endpoint,
            events: self.events_rx,
            handle,
        }
    }
}

/// A started mock side.
pub struct Started {
    pub endpoint: CoreEndpoint,
    /// This side's transport events (overflow / start / stop).
    pub events: Receiver<TransportEvent>,
    pub handle: TransportHandle,
}

impl Started {
    /// Stop pump threads, join them, and hand back the event receiver so
    /// terminal events (Stopped / Fatal) can still be drained.
    pub fn shutdown(self) -> Receiver<TransportEvent> {
        self.handle.shutdown();
        self.events
    }
}

/// Create a connected in-process transport pair with bounded frame queues.
///
/// Frames sent on side A's endpoint arrive at side B's endpoint and vice
/// versa. `capacity` bounds each direction independently.
pub fn connected_pair(capacity: usize) -> (MockSide, MockSide) {
    let a = MockTransport::new("mock-a");
    let b = MockTransport::new("mock-b");

    let (ev_a_tx, ev_a_rx) =
        crossbeam_channel::bounded::<TransportEvent>(EVENT_QUEUE_CAP);
    let (ev_b_tx, ev_b_rx) =
        crossbeam_channel::bounded::<TransportEvent>(EVENT_QUEUE_CAP);

    let (ctx_a, ep_a) = CoreEndpoint::channel(capacity, ev_a_tx, a.info().metrics.clone());
    let (ctx_b, ep_b) = CoreEndpoint::channel(capacity, ev_b_tx, b.info().metrics.clone());

    // Cross-wire inbound queues: A's pump feeds B's inbound slot and vice
    // versa (senders are cloned; each ctx keeps its own receiver).
    a.attach_peer(PeerLink {
        name: b.info().name,
        inbound: ctx_b.inbound.clone(),
        metrics: b.info().metrics.clone(),
    });
    b.attach_peer(PeerLink {
        name: a.info().name,
        inbound: ctx_a.inbound.clone(),
        metrics: a.info().metrics.clone(),
    });

    let side_a = MockSide {
        endpoint: ep_a,
        events_rx: ev_a_rx,
        transport: a,
        ctx: Some(ctx_a),
    };
    let side_b = MockSide {
        endpoint: ep_b,
        events_rx: ev_b_rx,
        transport: b,
        ctx: Some(ctx_b),
    };
    (side_a, side_b)
}
