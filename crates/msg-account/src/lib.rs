//! # msg-account
//!
//! Cash and position accounts for the brsk-msgx trading platform.
//!
//! Model: accounts are sharded by `account_id`. Each shard runs one
//! single-writer thread; commands arrive over a bounded MPSC queue and are
//! applied serially, so every compound operation (check available -> freeze,
//! fill -> average price -> PnL) is atomic without locks or CAS.
//!
//! Read paths come in two consistency levels:
//! - strong reads and trading decisions go through the command queue
//!   (query is serialized with writes, so "read then freeze" has no gap);
//! - monitoring / display reads use lock-free [`arc_swap`] snapshots and may
//!   be one event stale.
//!
//! Every mutating command carries a client-generated `req_id`. Duplicate
//! `req_id`s return the cached original outcome (idempotent retries and Raft
//! log replay). `req_id == 0` means "non-idempotent, always execute".
//!
//! All amounts are plain `i64` in the instrument's minor unit (cents /
//! contracts); wrap them with the `msg-domain` guarded types at the API edge.
//! Arithmetic is checked and overflows reject the command instead of
//! wrapping.

mod dedupe;
mod cash;
mod position;
mod shard;

pub use cash::{CashOp, CashSnapshot};
pub use position::{PosOp, PosReceipt, PositionSnapshot};
pub use shard::{
    AccountEvent, AccountShard, CashAccount, PositionAccount, ReplyReceiver, ShardCommand,
};

use std::fmt;

/// Errors produced by account commands. Rejections do not mutate state and do
/// not bump the ledger version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountError {
    /// Account / position is not open on this shard.
    UnknownAccount,
    /// Open command targets an already existing account.
    AccountExists,
    /// Amount / quantity argument is illegal (zero, negative where not
    /// allowed, frozen larger than the fill, ...).
    InvalidArgument(&'static str),
    /// Not enough *available* cash (`cash - frozen`).
    InsufficientCash { available: i64, requested: i64 },
    /// Not enough *available* position on the side.
    InsufficientPosition { available: i64, requested: i64 },
    /// Checked arithmetic overflowed.
    Overflow,
    /// Shard thread has stopped or its command queue is closed.
    ShardStopped,
}

impl fmt::Display for AccountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccountError::UnknownAccount => write!(f, "unknown account or position"),
            AccountError::AccountExists => write!(f, "account already exists"),
            AccountError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            AccountError::InsufficientCash {
                available,
                requested,
            } => write!(
                f,
                "insufficient available cash: requested {requested}, available {available}"
            ),
            AccountError::InsufficientPosition {
                available,
                requested,
            } => write!(
                f,
                "insufficient available position: requested {requested}, available {available}"
            ),
            AccountError::Overflow => write!(f, "arithmetic overflow"),
            AccountError::ShardStopped => write!(f, "account shard is stopped"),
        }
    }
}

impl std::error::Error for AccountError {}

/// Result type used throughout the crate.
pub type AccountResult<T> = Result<T, AccountError>;
