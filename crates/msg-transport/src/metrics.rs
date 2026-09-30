//! Lock-free per-transport counters, exported to Prometheus later.

use std::sync::atomic::{AtomicU64, Ordering};

/// Counters shared between transport threads, frame handles and the core.
/// All counters are monotonic; gauge-like state (queue depth) is read from
/// the channel at scrape time if needed.
#[derive(Debug, Default)]
pub struct TransportMetrics {
    /// Frames handed to the core (accepted by the inbound queue).
    pub rx_frames: AtomicU64,
    /// Frames the core gave to the transport to send.
    pub tx_frames: AtomicU64,
    pub rx_bytes: AtomicU64,
    pub tx_bytes: AtomicU64,
    /// Frames dropped because the inbound queue was full.
    pub rx_dropped: AtomicU64,
    /// `send` attempts rejected because the outbound queue was full.
    pub tx_full: AtomicU64,
    /// RxFrames dropped via `Drop` without an explicit `release()`: hot path
    /// code must release explicitly; this counter surfaces missing releases.
    pub rx_implicit_release: AtomicU64,
    /// Transport events dropped because the event channel was full.
    pub events_dropped: AtomicU64,
    /// Fatal errors observed by transport threads.
    pub fatal_errors: AtomicU64,
}

/// Plain-value snapshot for scraping/diffing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub rx_frames: u64,
    pub tx_frames: u64,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_dropped: u64,
    pub tx_full: u64,
    pub rx_implicit_release: u64,
    pub events_dropped: u64,
    pub fatal_errors: u64,
}

impl TransportMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            rx_frames: self.rx_frames.load(Ordering::Relaxed),
            tx_frames: self.tx_frames.load(Ordering::Relaxed),
            rx_bytes: self.rx_bytes.load(Ordering::Relaxed),
            tx_bytes: self.tx_bytes.load(Ordering::Relaxed),
            rx_dropped: self.rx_dropped.load(Ordering::Relaxed),
            tx_full: self.tx_full.load(Ordering::Relaxed),
            rx_implicit_release: self.rx_implicit_release.load(Ordering::Relaxed),
            events_dropped: self.events_dropped.load(Ordering::Relaxed),
            fatal_errors: self.fatal_errors.load(Ordering::Relaxed),
        }
    }
}
