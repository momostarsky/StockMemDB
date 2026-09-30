//! Bounded FIFO idempotency cache.
//!
//! Single-writer (the shard thread) only, so no synchronization of its own.
//! Remembers the last `cap` request ids together with their original outcome;
//! a retried command returns the cached outcome without touching the ledger.

use std::collections::{HashMap, VecDeque};

pub(crate) struct DedupeCache<T> {
    cap: usize,
    map: HashMap<u128, T>,
    order: VecDeque<u128>,
}

/// A request id of 0 opts out of idempotency: the command is always executed.
pub(crate) const NON_IDEMPOTENT: u128 = 0;

impl<T: Clone> DedupeCache<T> {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            map: HashMap::with_capacity(cap.min(4096)),
            order: VecDeque::with_capacity(cap.min(4096)),
        }
    }

    pub(crate) fn get(&self, req_id: u128) -> Option<T> {
        if req_id == NON_IDEMPOTENT {
            return None;
        }
        self.map.get(&req_id).cloned()
    }

    /// Store `value` under `req_id` and evict the oldest entry past capacity.
    pub(crate) fn remember(&mut self, req_id: u128, value: T) {
        if req_id != NON_IDEMPOTENT && !self.map.contains_key(&req_id) {
            self.order.push_back(req_id);
            self.map.insert(req_id, value);
            while self.map.len() > self.cap {
                if let Some(old) = self.order.pop_front() {
                    self.map.remove(&old);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replays_cached_outcome_and_evicts() {
        let mut c = DedupeCache::new(2);
        c.remember(1, 10u32);
        c.remember(2, 20u32);
        // Retry returns the original outcome.
        assert_eq!(c.get(1), Some(10));
        // Third entry evicts the oldest (1).
        c.remember(3, 30);
        assert_eq!(c.get(1), None);
        assert_eq!(c.get(3), Some(30));
        // Zero is never cached.
        assert_eq!(c.get(0), None);
        c.remember(0, 99);
        assert_eq!(c.get(0), None);
    }
}
