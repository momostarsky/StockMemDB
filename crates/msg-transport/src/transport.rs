//! Transport trait, capability flags, events and lifecycle handle.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::frame::TransportCtx;
use crate::metrics::TransportMetrics;

/// Capability bit flags returned by [`TransportInfo::caps`]. Used together
/// with [`TransportInfo::has`].
pub struct TransportCaps;

impl TransportCaps {
    /// Zero-copy receive into user buffers (AF_XDP UMEM / DPDK mbuf).
    pub const ZERO_COPY_RX: u8 = 1 << 0;
    /// NIC hardware receive timestamps available.
    pub const HW_TIMESTAMP: u8 = 1 << 1;
    /// Transport runs dedicated busy-polling (pinned) receive threads.
    pub const BUSY_POLL: u8 = 1 << 2;
    /// In-process implementation (mock / tests), no real network.
    pub const IN_PROCESS: u8 = 1 << 3;
}

/// Static-ish description plus live metrics of a transport.
pub struct TransportInfo {
    pub name: &'static str,
    pub caps: u8,
    pub metrics: Arc<TransportMetrics>,
}

impl TransportInfo {
    pub fn has(&self, cap: u8) -> bool {
        self.caps & cap != 0
    }
}

/// Health and overflow events delivered on the ctx event channel. The monitor
/// turns these into alerts; [`crate::mock::MockTransport`] also uses them so
/// tests can observe drop behaviour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportEvent {
    Started {
        name: String,
    },
    Stopped {
        name: String,
    },
    /// Inbound queue was full; one frame was dropped (cumulative counter in
    /// metrics too).
    InboundOverflow {
        name: String,
        dropped_total: u64,
    },
    Fatal {
        name: String,
        error: String,
    },
}

/// Cooperative stop flag shared with transport IO threads.
#[derive(Clone)]
pub struct StopToken {
    inner: Arc<AtomicBool>,
}

impl StopToken {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn stop(&self) {
        self.inner.store(true, Ordering::SeqCst);
    }

    pub fn is_stopped(&self) -> bool {
        self.inner.load(Ordering::SeqCst)
    }
}

impl Default for StopToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle returned by [`Transport::start`]; stopping joins the IO threads.
pub struct TransportHandle {
    name: String,
    stop: StopToken,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl TransportHandle {
    pub(crate) fn new(name: impl Into<String>, stop: StopToken) -> Self {
        Self {
            name: name.into(),
            stop,
            threads: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn add_thread(&self, handle: JoinHandle<()>) {
        self.threads.lock().expect("handle mutex").push(handle);
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn stop_token(&self) -> StopToken {
        self.stop.clone()
    }

    pub fn is_stopped(&self) -> bool {
        self.stop.is_stopped()
    }

    /// Signal threads to exit and join them. Idempotent.
    pub fn shutdown(self) {
        self.stop.stop();
        let mut threads = self.threads.lock().expect("handle mutex");
        for t in threads.drain(..) {
            let _ = t.join();
        }
    }
}

/// The single contract every transport implementation fulfils (Roadmap 6.1).
///
/// Implementations own their threads and buffers; everything crossing into
/// the core is a whole frame on the [`TransportCtx`] queues.
pub trait Transport: Send + Sync + 'static {
    fn info(&self) -> &TransportInfo;

    /// Wire the transport to its frame queues and spawn IO threads. The
    /// returned handle stops and joins them.
    fn start(self: Arc<Self>, ctx: TransportCtx) -> TransportHandle;
}
