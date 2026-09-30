//! Cash ledger: total / frozen money with compound invariant
//! `0 <= frozen <= cash`.

use crate::dedupe::{DedupeCache, NON_IDEMPOTENT};
use crate::{AccountError, AccountResult};

/// Default size of the per-ledger idempotency window.
pub(crate) const DEDUPE_CAP: usize = 4096;

/// Immutable cash state published to lock-free readers.
///
/// All amounts are `i64` in the currency minor unit (e.g. cents).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CashSnapshot {
    pub account_id: u64,
    pub version: u64,
    pub cash: i64,
    pub frozen: i64,
}

impl CashSnapshot {
    /// Available = cash - frozen. This is what pre-trade risk checks see.
    #[inline]
    pub fn available(&self) -> i64 {
        self.cash - self.frozen
    }
}

/// Cash operations requested by clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CashOp {
    /// Add settled cash into the account.
    Deposit(i64),
    /// Take cash out of *available* funds.
    Withdraw(i64),
    /// Freeze available cash (margin for a new order).
    Freeze(i64),
    /// Release previously frozen cash back to available (cancel / reject).
    Release(i64),
    /// Settle a fill: release `release` from frozen and move `net` out of
    /// cash. `net` is signed (positive = the account pays).
    Settle { release: i64, net: i64 },
}

/// Mutable ledger state, visible only to the owning shard thread.
pub(crate) struct CashLedger {
    account_id: u64,
    version: u64,
    cash: i64,
    frozen: i64,
    seen: DedupeCache<AccountResult<u64>>,
}

impl CashLedger {
    pub(crate) fn new(account_id: u64, initial_cash: i64) -> AccountResult<Self> {
        if initial_cash < 0 {
            return Err(AccountError::InvalidArgument("initial cash must be >= 0"));
        }
        Ok(Self {
            account_id,
            version: 0,
            cash: initial_cash,
            frozen: 0,
            seen: DedupeCache::new(DEDUPE_CAP),
        })
    }

    pub(crate) fn snapshot(&self) -> CashSnapshot {
        CashSnapshot {
            account_id: self.account_id,
            version: self.version,
            cash: self.cash,
            frozen: self.frozen,
        }
    }

    /// Apply an operation with idempotency. On success returns the new
    /// version; a rejection leaves the ledger untouched.
    pub(crate) fn apply(&mut self, req_id: u128, op: CashOp) -> AccountResult<u64> {
        if req_id != NON_IDEMPOTENT {
            if let Some(cached) = self.seen.get(req_id) {
                return cached;
            }
        }
        let outcome = self.execute(op);
        if req_id != NON_IDEMPOTENT {
            self.seen.remember(req_id, outcome.clone());
        }
        outcome
    }

    fn execute(&mut self, op: CashOp) -> AccountResult<u64> {
        match op {
            CashOp::Deposit(amount) => {
                if amount <= 0 {
                    return Err(AccountError::InvalidArgument("deposit amount must be > 0"));
                }
                self.cash = self
                    .cash
                    .checked_add(amount)
                    .ok_or(AccountError::Overflow)?;
            }
            CashOp::Withdraw(amount) => {
                if amount <= 0 {
                    return Err(AccountError::InvalidArgument("withdraw amount must be > 0"));
                }
                let available = self.cash - self.frozen;
                if amount > available {
                    return Err(AccountError::InsufficientCash {
                        available,
                        requested: amount,
                    });
                }
                self.cash = self
                    .cash
                    .checked_sub(amount)
                    .ok_or(AccountError::Overflow)?;
            }
            CashOp::Freeze(amount) => {
                if amount <= 0 {
                    return Err(AccountError::InvalidArgument("freeze amount must be > 0"));
                }
                let available = self.cash - self.frozen;
                if amount > available {
                    return Err(AccountError::InsufficientCash {
                        available,
                        requested: amount,
                    });
                }
                self.frozen = self
                    .frozen
                    .checked_add(amount)
                    .ok_or(AccountError::Overflow)?;
            }
            CashOp::Release(amount) => {
                if amount <= 0 {
                    return Err(AccountError::InvalidArgument("release amount must be > 0"));
                }
                if amount > self.frozen {
                    return Err(AccountError::InvalidArgument("release exceeds frozen"));
                }
                self.frozen -= amount;
            }
            CashOp::Settle { release, net } => {
                if release < 0 {
                    return Err(AccountError::InvalidArgument("release must be >= 0"));
                }
                if release > self.frozen {
                    return Err(AccountError::InvalidArgument("release exceeds frozen"));
                }
                // i128 intermediate to prove the post-state fits in i64.
                let new_frozen = self.frozen as i128 - release as i128;
                let new_cash = self.cash as i128 - net as i128;
                if new_frozen < 0 || new_frozen > new_cash {
                    return Err(AccountError::InsufficientCash {
                        available: self.cash - self.frozen,
                        requested: net.max(release),
                    });
                }
                if new_cash > i64::MAX as i128 || new_cash < i64::MIN as i128 {
                    return Err(AccountError::Overflow);
                }
                self.frozen = new_frozen as i64;
                self.cash = new_cash as i64;
            }
        }

        // Every successful mutation advances the version by exactly one.
        self.version = self
            .version
            .checked_add(1)
            .ok_or(AccountError::Overflow)?;
        Ok(self.version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn freeze_settle_lifecycle() {
        let mut l = CashLedger::new(1, 10_000).unwrap();
        assert_eq!(l.snapshot().available(), 10_000);

        assert_eq!(l.apply(1, CashOp::Freeze(3_000)).unwrap(), 1);
        assert_eq!(l.snapshot().frozen, 3_000);
        assert_eq!(l.snapshot().available(), 7_000);

        // Over-freeze is rejected and version is untouched.
        let err = l.apply(2, CashOp::Freeze(8_000)).unwrap_err();
        assert_eq!(
            err,
            AccountError::InsufficientCash {
                available: 7_000,
                requested: 8_000
            }
        );
        assert_eq!(l.snapshot().version, 1);

        // Settle: release 3000 frozen, pay 3000 cash.
        assert_eq!(
            l.apply(3, CashOp::Settle { release: 3_000, net: 3_000 })
                .unwrap(),
            2
        );
        assert_eq!(l.snapshot().cash, 7_000);
        assert_eq!(l.snapshot().frozen, 0);
    }

    #[test]
    fn duplicate_request_is_idempotent() {
        let mut l = CashLedger::new(1, 10_000).unwrap();
        let first = l.apply(7, CashOp::Freeze(1_000)).unwrap();
        // Same req_id retried: cached version, frozen only counted once.
        let second = l.apply(7, CashOp::Freeze(1_000)).unwrap();
        assert_eq!(first, second);
        assert_eq!(l.snapshot().frozen, 1_000);
        assert_eq!(l.snapshot().version, 1);
    }

    #[test]
    fn rejects_bad_arguments() {
        let mut l = CashLedger::new(1, 100).unwrap();
        assert!(matches!(
            CashLedger::new(1, -1),
            Err(AccountError::InvalidArgument(_))
        ));
        assert!(matches!(
            l.apply(1, CashOp::Freeze(0)),
            Err(AccountError::InvalidArgument(_))
        ));
        assert!(matches!(
            l.apply(2, CashOp::Release(50)),
            Err(AccountError::InvalidArgument(_))
        ));
        assert_eq!(l.snapshot().version, 0);
    }
}
