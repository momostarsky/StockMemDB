//! # msg-transport
//!
//! Transport layer contract for brsk-msgx (Roadmap section 6). All concrete
//! transports - io_uring TCP, AF_XDP and the cross-process DPDK/shm data plane
//! - implement the same [`Transport`] trait, so the message core is written
//!   once against frame queues and never sees sockets, XSK rings or mbufs.
//!
//! Four disciplines (Roadmap section 6.2):
//!
//! 1. **One queue item = one complete application frame.** TCP stream
//!    deframing happens *inside* the TCP transport; the core only ever gets
//!    whole frames (a complete `msg-proto` frame: 48-byte header + body).
//! 2. **Explicit frame ownership.** Received frames MUST be returned via
//!    [`RxFrame::release`]; `Drop` is a fallback that bumps a counter. With
//!    AF_XDP/DPDK the buffer returns to a UMEM ring / mempool instead of
//!    being freed.
//! 3. **Batch interfaces.** Both directions drain/enqueue in batches because
//!    every real backend (XSK rings, rte_ring, io_uring SQEs) is batched.
//! 4. **Transport-owned threads.** A transport owns its IO threads; the core
//!    talks to it only through bounded queues. The queues are bounded
//!    MPSC/MPMC channels today and can be swapped for lock-free SPSC rings
//!    without touching this API.
//!
//! ## Backpressure and overflow
//!
//! - Core -> transport: [`TxFrame::send`] fails fast with
//!   [`TransportError::QueueFull`] and hands the frame back when the outbound
//!   queue is full; the core applies its retry policy.
//! - Transport -> core: when the inbound queue is full the transport DROPS
//!   (counts + emits [`TransportEvent::InboundOverflow`]), mirroring AF_XDP
//!   ring semantics. A full inbound ring in production means: consume faster,
//!   bigger rings, or apply flow control - never silently lose observability.
//!
//! ## Current implementations
//!
//! - [`mock::MockTransport`]: in-process connected pair, runs the whole
//!   pipeline (real pump thread, batching, backpressure and overflow) on
//!   Windows/CI without Linux or NICs.
//! - IoUringTcpTransport / AfXdpTransport / DpdkShmTransport: later
//!   milestones behind the exact same contract.

mod frame;
mod metrics;
mod mock;
mod transport;

pub use frame::{
    CoreEndpoint, Inbound, Outbound, RxFrame, RxMeta, TransportCtx, TxFrame,
};
pub use metrics::{MetricsSnapshot, TransportMetrics};
pub use mock::{connected_pair, MockSide, MockTransport, Started};
pub use transport::{Transport, TransportCaps, TransportEvent, TransportHandle, TransportInfo};

use std::fmt;

/// Transport-layer errors.
#[derive(Debug)]
pub enum TransportError {
    /// Outbound queue is full; the unsent frame is returned for retry.
    QueueFull(TxFrame),
    /// The transport has stopped or its queue was closed.
    Stopped,
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::QueueFull(_) => write!(f, "transport outbound queue full"),
            TransportError::Stopped => write!(f, "transport stopped"),
        }
    }
}

impl std::error::Error for TransportError {}
