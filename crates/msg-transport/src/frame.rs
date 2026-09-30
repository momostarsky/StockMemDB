//! Frame handles, bidirectional link and the core-side endpoint.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError, TrySendError};

use crate::metrics::TransportMetrics;
use crate::transport::TransportEvent;
use crate::TransportError;

/// Wall-clock receive metadata attached to every inbound frame.
///
/// Software timestamp today; the AF_XDP implementation will additionally fill
/// a hardware NIC timestamp (Roadmap RxMeta: HW timestamp / RSS hash / queue
/// id).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RxMeta {
    /// Receive time as UTC epoch microseconds.
    pub recv_ts_us: i64,
    /// NIC receive-queue number when known (None for mock/TCP).
    pub queue_id: Option<u16>,
    /// RSS hash when the NIC provided one.
    pub rss_hash: Option<u32>,
    /// True if `recv_ts_us` came from NIC hardware timestamping.
    pub hardware_ts: bool,
}

impl RxMeta {
    pub fn software_now() -> Self {
        Self {
            recv_ts_us: now_epoch_micros(),
            queue_id: None,
            rss_hash: None,
            hardware_ts: false,
        }
    }
}

/// Backing storage of a received frame.
///
/// M0 only owns heap bytes. The enum exists so that a later AF_XDP variant
/// (frame referencing a UMEM chunk, returned to the FILL ring on release) can
/// be added without changing any call site.
#[derive(Debug)]
enum RxStorage {
    /// Complete wire frame (msg-proto header + body).
    Heap(Vec<u8>),
}

/// A received, whole application frame.
///
/// Borrow the bytes, then call [`RxFrame::release`] exactly one time. Dropping
/// without releasing works but bumps
/// [`TransportMetrics::rx_implicit_release`]; hot paths must not rely on it.
#[derive(Debug)]
pub struct RxFrame {
    storage: RxStorage,
    released: bool,
    metrics: Arc<TransportMetrics>,
}

impl RxFrame {
    pub(crate) fn heap(bytes: Vec<u8>, metrics: Arc<TransportMetrics>) -> Self {
        Self {
            storage: RxStorage::Heap(bytes),
            released: false,
            metrics,
        }
    }

    /// Complete frame bytes (48-byte msg-proto header + body).
    #[inline]
    pub fn payload(&self) -> &[u8] {
        match &self.storage {
            RxStorage::Heap(v) => v.as_slice(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.payload().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the buffer to the transport (free / UMEM chunk / mbuf).
    #[inline]
    pub fn release(mut self) {
        self.released = true;
        // Storage-specific return happens in Drop with the flag set.
    }
}

impl Drop for RxFrame {
    fn drop(&mut self) {
        if !self.released {
            // Fallback path: the buffer is still returned correctly, but the
            // hot path forgot the explicit release.
            self.metrics.rx_implicit_release.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// One core -> transport item.
#[derive(Debug)]
pub struct Outbound {
    pub frame: TxFrame,
}

/// One transport -> core item.
#[derive(Debug)]
pub struct Inbound {
    pub frame: RxFrame,
    pub meta: RxMeta,
}

/// A writable transmit buffer. Allocate from [`CoreEndpoint::alloc`], fill
/// with the complete wire frame, then [`TxFrame::send`].
#[derive(Debug)]
pub struct TxFrame {
    buf: Vec<u8>,
    outbound: Sender<Outbound>,
    metrics: Arc<TransportMetrics>,
}

impl TxFrame {
    pub(crate) fn new(
        len: usize,
        outbound: Sender<Outbound>,
        metrics: Arc<TransportMetrics>,
    ) -> Self {
        Self {
            buf: vec![0u8; len],
            outbound,
            metrics,
        }
    }

    /// Writable bytes of exactly the allocated size.
    #[inline]
    pub fn payload_mut(&mut self) -> &mut [u8] {
        self.buf.as_mut_slice()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Copy bytes into the frame. Must match the allocated size; hot paths
    /// write into [`TxFrame::payload_mut`] directly instead.
    pub fn write_from(&mut self, src: &[u8]) {
        assert_eq!(src.len(), self.buf.len(), "frame size mismatch");
        self.buf.copy_from_slice(src);
    }

    /// Consume into the backing bytes (transport pump implementations only).
    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    /// Enqueue for transmission. On a full queue the frame is handed back
    /// unchanged inside [`TransportError::QueueFull`] for the core retry
    /// policy (Roadmap section 6: backpressure is explicit, never blocking).
    pub fn send(self) -> Result<(), TransportError> {
        let len = self.buf.len();
        let metrics = self.metrics.clone();
        let outbound = self.outbound.clone();
        match outbound.try_send(Outbound { frame: self }) {
            Ok(()) => {
                metrics.tx_frames.fetch_add(1, Ordering::Relaxed);
                metrics.tx_bytes.fetch_add(len as u64, Ordering::Relaxed);
                Ok(())
            }
            Err(TrySendError::Full(Outbound { frame })) => {
                frame.metrics.tx_full.fetch_add(1, Ordering::Relaxed);
                Err(TransportError::QueueFull(frame))
            }
            // The buffer drops and is freed on disconnect.
            Err(TrySendError::Disconnected(Outbound { .. })) => Err(TransportError::Stopped),
        }
    }
}

/// What a transport implementation receives at [`crate::Transport::start`].
pub struct TransportCtx {
    /// Transport pushes inbound frames here.
    pub inbound: Sender<Inbound>,
    /// Transport drains frames the core wants sent from here.
    pub outbound: Receiver<Outbound>,
    /// Health/overflow events (best-effort; must never block IO threads).
    pub events: Sender<TransportEvent>,
}

/// Core-side handle to one transport link.
#[derive(Clone)]
pub struct CoreEndpoint {
    inbound: Receiver<Inbound>,
    outbound: Sender<Outbound>,
    metrics: Arc<TransportMetrics>,
}

impl CoreEndpoint {
    /// Build the two sides of one link. `events` is shared with the monitor;
    /// callers keep the receiving end.
    pub fn channel(
        capacity: usize,
        events: Sender<TransportEvent>,
        metrics: Arc<TransportMetrics>,
    ) -> (TransportCtx, CoreEndpoint) {
        let (in_tx, in_rx) = bounded::<Inbound>(capacity);
        let (out_tx, out_rx) = bounded::<Outbound>(capacity);
        let ctx = TransportCtx {
            inbound: in_tx,
            outbound: out_rx,
            events,
        };
        let endpoint = CoreEndpoint {
            inbound: in_rx,
            outbound: out_tx,
            metrics,
        };
        (ctx, endpoint)
    }

    pub fn metrics(&self) -> &Arc<TransportMetrics> {
        &self.metrics
    }

    /// Allocate a transmit buffer of exactly `len` bytes.
    #[inline]
    pub fn alloc(&self, len: usize) -> TxFrame {
        TxFrame::new(len, self.outbound.clone(), self.metrics.clone())
    }

    /// Convenience: build a frame, copy `bytes`, enqueue it.
    pub fn send_bytes(&self, bytes: &[u8]) -> Result<(), TransportError> {
        let mut f = self.alloc(bytes.len());
        f.write_from(bytes);
        f.send()
    }

    /// Non-blocking single receive.
    pub fn try_recv(&self) -> Result<Inbound, TryRecvError> {
        self.inbound.try_recv()
    }

    /// Drain up to `max` inbound frames in one batch. Returns the count;
    /// frames are appended to `out`.
    pub fn recv_batch(&self, out: &mut Vec<Inbound>, max: usize) -> usize {
        let mut n = 0;
        while n < max {
            match self.inbound.try_recv() {
                Ok(item) => {
                    out.push(item);
                    n += 1;
                }
                Err(_) => break,
            }
        }
        n
    }

    /// Approximate inbound queue depth (observability only).
    pub fn inbound_len(&self) -> usize {
        self.inbound.len()
    }

    pub fn outbound_len(&self) -> usize {
        self.outbound.len()
    }
}

/// Monotonic UTC epoch microseconds from the OS clock (no deps; the
/// AF_XDP/monoio path may inject a different clock source).
pub(crate) fn now_epoch_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
