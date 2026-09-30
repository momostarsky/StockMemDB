//! Position ledger for one `(account, instrument)`.
//!
//! Tracks long/short quantities, frozen quantities (reserved by working
//! orders), weighted average open prices and realized PnL.
//!
//! Conventions:
//! - quantities are whole units (`i64`, instrument qty scale resolved by the
//!   caller); prices are raw `i64` in the instrument price scale;
//! - a buy first closes the short side (realized PnL), the remainder opens
//!   long; a sell is symmetric;
//! - `release_frozen` on a fill is the quantity of the matching side freeze
//!   consumed by this fill (e.g. a long-closing sell releases long freeze).

use crate::cash::DEDUPE_CAP;
use crate::dedupe::{DedupeCache, NON_IDEMPOTENT};
use crate::{AccountError, AccountResult};

/// Immutable position state published to lock-free readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositionSnapshot {
    pub account_id: u64,
    pub instrument_id: u32,
    pub version: u64,
    pub long_qty: i64,
    pub long_frozen: i64,
    pub short_qty: i64,
    pub short_frozen: i64,
    /// Weighted average open price of the long side (price scale units).
    pub avg_long: i64,
    /// Weighted average open price of the short side (price scale units).
    pub avg_short: i64,
    /// Cumulative realized PnL (minor currency unit).
    pub realized_pnl: i64,
}

impl PositionSnapshot {
    #[inline]
    pub fn long_available(&self) -> i64 {
        self.long_qty - self.long_frozen
    }

    #[inline]
    pub fn short_available(&self) -> i64 {
        self.short_qty - self.short_frozen
    }

    /// Net position: long positive, short negative.
    #[inline]
    pub fn net_qty(&self) -> i64 {
        self.long_qty - self.short_qty
    }
}

/// Reply of a mutating position command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PosReceipt {
    pub version: u64,
    /// Realized PnL produced by this command (0 for freeze/release).
    pub realized_pnl_delta: i64,
    pub avg_long: i64,
    pub avg_short: i64,
}

/// Position operations requested by clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PosOp {
    /// Reserve long quantity for a sell order.
    FreezeSell(i64),
    /// Give back long reservation (order canceled / expired).
    ReleaseSell(i64),
    /// Reserve short quantity for a short-sell buy-cover model.
    FreezeBuy(i64),
    ReleaseBuy(i64),
    /// Buy fill: covers short first, then opens long.
    FillBuy {
        qty: i64,
        price: i64,
        release_frozen: i64,
    },
    /// Sell fill: closes long first, then opens short.
    FillSell {
        qty: i64,
        price: i64,
        release_frozen: i64,
    },
}

/// Mutable ledger state, visible only to the owning shard thread.
pub(crate) struct PositionLedger {
    account_id: u64,
    instrument_id: u32,
    version: u64,
    long_qty: i64,
    long_frozen: i64,
    short_qty: i64,
    short_frozen: i64,
    avg_long: i64,
    avg_short: i64,
    realized_pnl: i64,
    seen: DedupeCache<AccountResult<PosReceipt>>,
}

impl PositionLedger {
    pub(crate) fn new(account_id: u64, instrument_id: u32) -> Self {
        Self {
            account_id,
            instrument_id,
            version: 0,
            long_qty: 0,
            long_frozen: 0,
            short_qty: 0,
            short_frozen: 0,
            avg_long: 0,
            avg_short: 0,
            realized_pnl: 0,
            seen: DedupeCache::new(DEDUPE_CAP),
        }
    }

    pub(crate) fn snapshot(&self) -> PositionSnapshot {
        PositionSnapshot {
            account_id: self.account_id,
            instrument_id: self.instrument_id,
            version: self.version,
            long_qty: self.long_qty,
            long_frozen: self.long_frozen,
            short_qty: self.short_qty,
            short_frozen: self.short_frozen,
            avg_long: self.avg_long,
            avg_short: self.avg_short,
            realized_pnl: self.realized_pnl,
        }
    }

    pub(crate) fn apply(&mut self, req_id: u128, op: PosOp) -> AccountResult<PosReceipt> {
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

    fn freeze(&self, frozen: i64, qty: i64, qty_field: i64) -> AccountResult<()> {
        if qty <= 0 {
            return Err(AccountError::InvalidArgument("freeze qty must be > 0"));
        }
        let available = qty_field - frozen;
        if qty > available {
            return Err(AccountError::InsufficientPosition {
                available,
                requested: qty,
            });
        }
        Ok(())
    }

    fn execute(&mut self, op: PosOp) -> AccountResult<PosReceipt> {
        let mut pnl_delta: i128 = 0;

        match op {
            PosOp::FreezeSell(qty) => {
                self.freeze(self.long_frozen, qty, self.long_qty)?;
                self.long_frozen = self
                    .long_frozen
                    .checked_add(qty)
                    .ok_or(AccountError::Overflow)?;
            }
            PosOp::ReleaseSell(qty) => {
                if qty <= 0 || qty > self.long_frozen {
                    return Err(AccountError::InvalidArgument(
                        "release qty must be > 0 and <= frozen",
                    ));
                }
                self.long_frozen -= qty;
            }
            PosOp::FreezeBuy(qty) => {
                self.freeze(self.short_frozen, qty, self.short_qty)?;
                self.short_frozen = self
                    .short_frozen
                    .checked_add(qty)
                    .ok_or(AccountError::Overflow)?;
            }
            PosOp::ReleaseBuy(qty) => {
                if qty <= 0 || qty > self.short_frozen {
                    return Err(AccountError::InvalidArgument(
                        "release qty must be > 0 and <= frozen",
                    ));
                }
                self.short_frozen -= qty;
            }
            PosOp::FillBuy {
                qty,
                price,
                release_frozen,
            } => {
                self.validate_fill(qty, price, release_frozen, self.short_frozen)?;
                let close = qty.min(self.short_qty);
                if release_frozen > close {
                    return Err(AccountError::InvalidArgument(
                        "release_frozen exceeds the quantity closed",
                    ));
                }
                // Cover short: buy back at price below avg_short earns PnL.
                pnl_delta = close as i128 * (self.avg_short as i128 - price as i128);
                self.short_qty -= close;
                self.short_frozen -= release_frozen;
                if self.short_qty == 0 {
                    if self.short_frozen != 0 {
                        return Err(AccountError::Overflow);
                    }
                    self.avg_short = 0;
                }
                let open = qty - close;
                if open > 0 {
                    self.avg_long = weighted_avg(self.long_qty, self.avg_long, open, price)?;
                    self.long_qty = self
                        .long_qty
                        .checked_add(open)
                        .ok_or(AccountError::Overflow)?;
                }
            }
            PosOp::FillSell {
                qty,
                price,
                release_frozen,
            } => {
                self.validate_fill(qty, price, release_frozen, self.long_frozen)?;
                let close = qty.min(self.long_qty);
                if release_frozen > close {
                    return Err(AccountError::InvalidArgument(
                        "release_frozen exceeds the quantity closed",
                    ));
                }
                // Close long: sell above avg_long earns PnL.
                pnl_delta = close as i128 * (price as i128 - self.avg_long as i128);
                self.long_qty -= close;
                self.long_frozen -= release_frozen;
                if self.long_qty == 0 {
                    if self.long_frozen != 0 {
                        return Err(AccountError::Overflow);
                    }
                    self.avg_long = 0;
                }
                let open = qty - close;
                if open > 0 {
                    self.avg_short =
                        weighted_avg(self.short_qty, self.avg_short, open, price)?;
                    self.short_qty = self
                        .short_qty
                        .checked_add(open)
                        .ok_or(AccountError::Overflow)?;
                }
            }
        }

        self.realized_pnl = i128_to_i64(
            self.realized_pnl as i128 + pnl_delta,
        )?;
        let pnl_delta_i64 = i128_to_i64(pnl_delta)?;
        self.version = self
            .version
            .checked_add(1)
            .ok_or(AccountError::Overflow)?;

        self.assert_invariants();
        Ok(PosReceipt {
            version: self.version,
            realized_pnl_delta: pnl_delta_i64,
            avg_long: self.avg_long,
            avg_short: self.avg_short,
        })
    }

    fn validate_fill(
        &self,
        qty: i64,
        price: i64,
        release_frozen: i64,
        frozen: i64,
    ) -> AccountResult<()> {
        if qty <= 0 {
            return Err(AccountError::InvalidArgument("fill qty must be > 0"));
        }
        if price < 0 {
            return Err(AccountError::InvalidArgument("fill price must be >= 0"));
        }
        if release_frozen < 0 || release_frozen > qty || release_frozen > frozen {
            return Err(AccountError::InvalidArgument(
                "release_frozen must be in 0..=min(qty, frozen)",
            ));
        }
        Ok(())
    }

    fn assert_invariants(&self) {
        debug_assert!(self.long_qty >= 0 && self.short_qty >= 0);
        debug_assert!(self.long_frozen >= 0 && self.short_frozen >= 0);
        debug_assert!(self.long_frozen <= self.long_qty);
        debug_assert!(self.short_frozen <= self.short_qty);
        debug_assert!(self.long_qty > 0 || self.avg_long == 0);
        debug_assert!(self.short_qty > 0 || self.avg_short == 0);
    }
}

/// Weighted average after adding `added_qty` at `added_price` to a position.
fn weighted_avg(
    old_qty: i64,
    old_avg: i64,
    added_qty: i64,
    added_price: i64,
) -> AccountResult<i64> {
    let numerator = old_qty as i128 * old_avg as i128
        + added_qty as i128 * added_price as i128;
    let total = old_qty as i128 + added_qty as i128;
    i128_to_i64(numerator / total)
}

fn i128_to_i64(v: i128) -> AccountResult<i64> {
    if v > i64::MAX as i128 || v < i64::MIN as i128 {
        Err(AccountError::Overflow)
    } else {
        Ok(v as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weighted_average_and_realized_pnl() {
        let mut p = PositionLedger::new(1, 42);
        // Buy 100 @ 1000, then 100 @ 1100 -> avg 1050.
        let r = p.apply(1, PosOp::FillBuy { qty: 100, price: 1000, release_frozen: 0 }).unwrap();
        assert_eq!(r.avg_long, 1000);
        let r = p.apply(2, PosOp::FillBuy { qty: 100, price: 1100, release_frozen: 0 }).unwrap();
        assert_eq!(r.avg_long, 1050);
        assert_eq!(p.snapshot().long_qty, 200);

        // Freeze and sell 120 @ 1100 -> realized = 120 * (1100 - 1050).
        p.apply(3, PosOp::FreezeSell(120)).unwrap();
        let r = p
            .apply(
                4,
                PosOp::FillSell {
                    qty: 120,
                    price: 1100,
                    release_frozen: 120,
                },
            )
            .unwrap();
        assert_eq!(r.realized_pnl_delta, 6_000);
        let s = p.snapshot();
        assert_eq!(s.long_qty, 80);
        assert_eq!(s.long_frozen, 0);
        assert_eq!(s.avg_long, 1050); // untouched when only closing
        assert_eq!(s.realized_pnl, 6_000);
    }

    #[test]
    fn freeze_prevents_oversell_and_is_released() {
        let mut p = PositionLedger::new(1, 42);
        p.apply(1, PosOp::FillBuy { qty: 200, price: 10, release_frozen: 0 }).unwrap();
        p.apply(2, PosOp::FreezeSell(150)).unwrap();
        // Only 50 remains available.
        assert_eq!(
            p.apply(3, PosOp::FreezeSell(60)).unwrap_err(),
            AccountError::InsufficientPosition {
                available: 50,
                requested: 60
            }
        );
        p.apply(4, PosOp::ReleaseSell(150)).unwrap();
        assert_eq!(p.snapshot().long_available(), 200);
    }

    #[test]
    fn buy_covers_short_with_pnl() {
        let mut p = PositionLedger::new(1, 9);
        // Open short 100 @ 1000.
        p.apply(
            1,
            PosOp::FillSell { qty: 100, price: 1000, release_frozen: 0 },
        )
        .unwrap();
        // Buy back 100 @ 980 -> profit 2000.
        let r = p
            .apply(
                2,
                PosOp::FillBuy { qty: 100, price: 980, release_frozen: 0 },
            )
            .unwrap();
        assert_eq!(r.realized_pnl_delta, 2_000);
        assert_eq!(p.snapshot().net_qty(), 0);
        assert_eq!(p.snapshot().avg_short, 0);
    }

    #[test]
    fn duplicate_fill_counts_once() {
        let mut p = PositionLedger::new(1, 42);
        p.apply(1, PosOp::FillBuy { qty: 100, price: 1000, release_frozen: 0 }).unwrap();
        p.apply(2, PosOp::FreezeSell(100)).unwrap();
        let cmd = PosOp::FillSell { qty: 100, price: 1010, release_frozen: 100 };
        let a = p.apply(9, cmd).unwrap();
        let b = p.apply(9, cmd).unwrap();
        assert_eq!(a, b);
        assert_eq!(p.snapshot().long_qty, 0);
        assert_eq!(p.snapshot().realized_pnl, 1_000);
        assert_eq!(p.snapshot().version, 3);
    }
}
