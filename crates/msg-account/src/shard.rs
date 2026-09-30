//! Single-writer account shard: one pinned command loop per shard, lock-free
//! snapshot publishing via `arc-swap`, optional event stream for WAL /
//! monitoring.

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use arc_swap::ArcSwap;
use crossbeam_channel::{bounded, Receiver, Sender};

use crate::cash::{CashLedger, CashOp, CashSnapshot};
use crate::position::{PosOp, PosReceipt, PositionLedger, PositionSnapshot};
use crate::{AccountError, AccountResult};

/// Bounded command queue depth per shard. A full queue blocks producers,
/// providing natural back-pressure.
const QUEUE_CAP: usize = 65_536;

/// Receiver side of a one-shot command reply.
pub type ReplyReceiver<T> = Receiver<T>;

/// Commands accepted by a shard thread.
pub enum ShardCommand {
    /// Open a cash account. The client supplies the snapshot slot so it can
    /// keep a lock-free read handle.
    OpenCash {
        account_id: u64,
        initial_cash: i64,
        snap: Arc<ArcSwap<CashSnapshot>>,
        reply: Sender<AccountResult<()>>,
    },
    /// Open a zero position for `(account, instrument)`.
    OpenPosition {
        account_id: u64,
        instrument_id: u32,
        snap: Arc<ArcSwap<PositionSnapshot>>,
        reply: Sender<AccountResult<()>>,
    },
    /// Apply a cash operation.
    Cash {
        account_id: u64,
        req_id: u128,
        op: CashOp,
        reply: Sender<AccountResult<u64>>,
    },
    /// Apply a position operation.
    Position {
        account_id: u64,
        instrument_id: u32,
        req_id: u128,
        op: PosOp,
        reply: Sender<AccountResult<PosReceipt>>,
    },
    /// Strongly consistent snapshot read (serialized ahead of later writes).
    CashSnapshot {
        account_id: u64,
        reply: Sender<AccountResult<CashSnapshot>>,
    },
    PositionSnapshot {
        account_id: u64,
        instrument_id: u32,
        reply: Sender<AccountResult<PositionSnapshot>>,
    },
    /// Stop the shard thread.
    Stop,
}

/// Emitted after every successful mutation; feed it to the WAL appender and/or
/// monitoring fan-out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountEvent {
    CashChanged(CashSnapshot),
    PositionChanged(PositionSnapshot),
}

/// Handle to a running shard. Cheap to clone; `Clone` shares the queue.
#[derive(Clone)]
pub struct AccountShard {
    id: usize,
    tx: Sender<ShardCommand>,
}

impl AccountShard {
    /// Spawn one shard thread. `events` receives one event per successful
    /// mutation (pass `None` to disable).
    pub fn spawn(
        id: usize,
        events: Option<Sender<AccountEvent>>,
    ) -> std::io::Result<AccountShard> {
        let (tx, rx) = bounded::<ShardCommand>(QUEUE_CAP);
        let builder = thread::Builder::new().name(format!("account-shard-{id}"));
        builder.spawn(move || run(rx, events))?;
        Ok(AccountShard { id, tx })
    }

    pub fn id(&self) -> usize {
        self.id
    }

    /// Choose the shard for an account. Number of shards should equal the
    /// number of dedicated worker cores.
    pub fn shard_for(account_id: u64, shard_count: usize) -> usize {
        debug_assert!(shard_count > 0);
        (account_id % shard_count as u64) as usize
    }

    pub(crate) fn send(&self, cmd: ShardCommand) -> AccountResult<()> {
        self.tx.send(cmd).map_err(|_| AccountError::ShardStopped)
    }

    /// Ask the shard to stop and wait for its thread to finish.
    pub fn stop(self) {
        let _ = self.tx.send(ShardCommand::Stop);
    }
}

/// Mutable state owned exclusively by the shard thread.
type CashEntry = (CashLedger, Arc<ArcSwap<CashSnapshot>>);
type PositionKey = (u64, u32);
type PositionEntry = (PositionLedger, Arc<ArcSwap<PositionSnapshot>>);

struct ShardState {
    cash: HashMap<u64, CashEntry>,
    positions: HashMap<PositionKey, PositionEntry>,
    events: Option<Sender<AccountEvent>>,
}

impl ShardState {
    fn emit(&self, event: AccountEvent) {
        if let Some(tx) = &self.events {
            // Monitor backlog must never block the accounting thread.
            let _ = tx.try_send(event);
        }
    }
}

fn run(rx: Receiver<ShardCommand>, events: Option<Sender<AccountEvent>>) {
    let mut state = ShardState {
        cash: HashMap::new(),
        positions: HashMap::new(),
        events,
    };

    while let Ok(cmd) = rx.recv() {
        match cmd {
            ShardCommand::OpenCash {
                account_id,
                initial_cash,
                snap,
                reply,
            } => {
                let result = match CashLedger::new(account_id, initial_cash) {
                    Ok(ledger) => match state.cash.entry(account_id) {
                        std::collections::hash_map::Entry::Vacant(e) => {
                            snap.store(Arc::new(ledger.snapshot()));
                            e.insert((ledger, snap));
                            Ok(())
                        }
                        std::collections::hash_map::Entry::Occupied(_) => {
                            Err(AccountError::AccountExists)
                        }
                    },
                    Err(e) => Err(e),
                };
                let _ = reply.send(result);
            }
            ShardCommand::OpenPosition {
                account_id,
                instrument_id,
                snap,
                reply,
            } => {
                let key = (account_id, instrument_id);
                let result = match state.positions.entry(key) {
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let ledger = PositionLedger::new(account_id, instrument_id);
                        snap.store(Arc::new(ledger.snapshot()));
                        e.insert((ledger, snap));
                        Ok(())
                    }
                    std::collections::hash_map::Entry::Occupied(_) => {
                        Err(AccountError::AccountExists)
                    }
                };
                let _ = reply.send(result);
            }
            ShardCommand::Cash {
                account_id,
                req_id,
                op,
                reply,
            } => {
                let result = match state.cash.get_mut(&account_id) {
                    Some((ledger, slot)) => {
                        let outcome = ledger.apply(req_id, op);
                        if outcome.is_ok() {
                            let snapshot = ledger.snapshot();
                            slot.store(Arc::new(snapshot));
                            state.emit(AccountEvent::CashChanged(snapshot));
                        }
                        outcome
                    }
                    None => Err(AccountError::UnknownAccount),
                };
                let _ = reply.send(result);
            }
            ShardCommand::Position {
                account_id,
                instrument_id,
                req_id,
                op,
                reply,
            } => {
                let result = match state.positions.get_mut(&(account_id, instrument_id)) {
                    Some((ledger, slot)) => {
                        let outcome = ledger.apply(req_id, op);
                        if outcome.is_ok() {
                            let snapshot = ledger.snapshot();
                            slot.store(Arc::new(snapshot));
                            state.emit(AccountEvent::PositionChanged(snapshot));
                        }
                        outcome
                    }
                    None => Err(AccountError::UnknownAccount),
                };
                let _ = reply.send(result);
            }
            ShardCommand::CashSnapshot { account_id, reply } => {
                let result = state
                    .cash
                    .get(&account_id)
                    .map(|(ledger, _)| ledger.snapshot())
                    .ok_or(AccountError::UnknownAccount);
                let _ = reply.send(result);
            }
            ShardCommand::PositionSnapshot {
                account_id,
                instrument_id,
                reply,
            } => {
                let result = state
                    .positions
                    .get(&(account_id, instrument_id))
                    .map(|(ledger, _)| ledger.snapshot())
                    .ok_or(AccountError::UnknownAccount);
                let _ = reply.send(result);
            }
            ShardCommand::Stop => break,
        }
    }
}

// ---------------------------------------------------------------------------
// Client handles
// ---------------------------------------------------------------------------

/// Client handle for one cash account. Holds the lock-free snapshot slot.
pub struct CashAccount {
    account_id: u64,
    cmd: Sender<ShardCommand>,
    snap: Arc<ArcSwap<CashSnapshot>>,
}

impl CashAccount {
    /// Open an account on a shard and wait for confirmation.
    pub fn open(
        shard: &AccountShard,
        account_id: u64,
        initial_cash: i64,
    ) -> AccountResult<CashAccount> {
        let initial = CashSnapshot {
            account_id,
            version: 0,
            cash: initial_cash,
            frozen: 0,
        };
        let snap = Arc::new(ArcSwap::from_pointee(initial));
        let (tx, rx) = bounded::<AccountResult<()>>(1);
        shard.send(ShardCommand::OpenCash {
            account_id,
            initial_cash,
            snap: snap.clone(),
            reply: tx,
        })?;
        rx.recv().map_err(|_| AccountError::ShardStopped)??;
        Ok(CashAccount {
            account_id,
            cmd: shard.tx.clone(),
            snap,
        })
    }

    pub fn account_id(&self) -> u64 {
        self.account_id
    }

    /// Lock-free read: may be one event stale, never blocks the writer.
    #[inline]
    pub fn current(&self) -> CashSnapshot {
        **self.snap.load()
    }

    /// Strong read through the command queue (ordered with mutations).
    pub fn snapshot_blocking(&self) -> AccountResult<CashSnapshot> {
        let (tx, rx) = bounded::<AccountResult<CashSnapshot>>(1);
        self.cmd
            .send(ShardCommand::CashSnapshot {
                account_id: self.account_id,
                reply: tx,
            })
            .map_err(|_| AccountError::ShardStopped)?;
        rx.recv().map_err(|_| AccountError::ShardStopped)?
    }

    /// Queue a cash operation; await the receiver however the runtime likes.
    pub fn call(
        &self,
        req_id: u128,
        op: CashOp,
    ) -> AccountResult<ReplyReceiver<AccountResult<u64>>> {
        let (tx, rx) = bounded(1);
        self.cmd
            .send(ShardCommand::Cash {
                account_id: self.account_id,
                req_id,
                op,
                reply: tx,
            })
            .map_err(|_| AccountError::ShardStopped)?;
        Ok(rx)
    }

    pub fn call_blocking(&self, req_id: u128, op: CashOp) -> AccountResult<u64> {
        self.call(req_id, op)?
            .recv()
            .map_err(|_| AccountError::ShardStopped)?
    }

    pub fn deposit_blocking(&self, req_id: u128, amount: i64) -> AccountResult<u64> {
        self.call_blocking(req_id, CashOp::Deposit(amount))
    }

    pub fn withdraw_blocking(&self, req_id: u128, amount: i64) -> AccountResult<u64> {
        self.call_blocking(req_id, CashOp::Withdraw(amount))
    }

    pub fn freeze_blocking(&self, req_id: u128, amount: i64) -> AccountResult<u64> {
        self.call_blocking(req_id, CashOp::Freeze(amount))
    }

    pub fn release_blocking(&self, req_id: u128, amount: i64) -> AccountResult<u64> {
        self.call_blocking(req_id, CashOp::Release(amount))
    }

    pub fn settle_blocking(
        &self,
        req_id: u128,
        release: i64,
        net: i64,
    ) -> AccountResult<u64> {
        self.call_blocking(req_id, CashOp::Settle { release, net })
    }
}

/// Client handle for one `(account, instrument)` position.
pub struct PositionAccount {
    account_id: u64,
    instrument_id: u32,
    cmd: Sender<ShardCommand>,
    snap: Arc<ArcSwap<PositionSnapshot>>,
}

impl PositionAccount {
    pub fn open(
        shard: &AccountShard,
        account_id: u64,
        instrument_id: u32,
    ) -> AccountResult<PositionAccount> {
        let initial = PositionSnapshot {
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
        };
        let snap = Arc::new(ArcSwap::from_pointee(initial));
        let (tx, rx) = bounded::<AccountResult<()>>(1);
        shard.send(ShardCommand::OpenPosition {
            account_id,
            instrument_id,
            snap: snap.clone(),
            reply: tx,
        })?;
        rx.recv().map_err(|_| AccountError::ShardStopped)??;
        Ok(PositionAccount {
            account_id,
            instrument_id,
            cmd: shard.tx.clone(),
            snap,
        })
    }

    pub fn account_id(&self) -> u64 {
        self.account_id
    }

    pub fn instrument_id(&self) -> u32 {
        self.instrument_id
    }

    /// Lock-free read.
    #[inline]
    pub fn current(&self) -> PositionSnapshot {
        **self.snap.load()
    }

    /// Strong read through the command queue.
    pub fn snapshot_blocking(&self) -> AccountResult<PositionSnapshot> {
        let (tx, rx) = bounded::<AccountResult<PositionSnapshot>>(1);
        self.cmd
            .send(ShardCommand::PositionSnapshot {
                account_id: self.account_id,
                instrument_id: self.instrument_id,
                reply: tx,
            })
            .map_err(|_| AccountError::ShardStopped)?;
        rx.recv().map_err(|_| AccountError::ShardStopped)?
    }

    pub fn call(
        &self,
        req_id: u128,
        op: PosOp,
    ) -> AccountResult<ReplyReceiver<AccountResult<PosReceipt>>> {
        let (tx, rx) = bounded(1);
        self.cmd
            .send(ShardCommand::Position {
                account_id: self.account_id,
                instrument_id: self.instrument_id,
                req_id,
                op,
                reply: tx,
            })
            .map_err(|_| AccountError::ShardStopped)?;
        Ok(rx)
    }

    pub fn call_blocking(&self, req_id: u128, op: PosOp) -> AccountResult<PosReceipt> {
        self.call(req_id, op)?
            .recv()
            .map_err(|_| AccountError::ShardStopped)?
    }

    pub fn freeze_sell_blocking(&self, req_id: u128, qty: i64) -> AccountResult<PosReceipt> {
        self.call_blocking(req_id, PosOp::FreezeSell(qty))
    }

    pub fn release_sell_blocking(&self, req_id: u128, qty: i64) -> AccountResult<PosReceipt> {
        self.call_blocking(req_id, PosOp::ReleaseSell(qty))
    }

    pub fn fill_buy_blocking(
        &self,
        req_id: u128,
        qty: i64,
        price: i64,
        release_frozen: i64,
    ) -> AccountResult<PosReceipt> {
        self.call_blocking(
            req_id,
            PosOp::FillBuy {
                qty,
                price,
                release_frozen,
            },
        )
    }

    pub fn fill_sell_blocking(
        &self,
        req_id: u128,
        qty: i64,
        price: i64,
        release_frozen: i64,
    ) -> AccountResult<PosReceipt> {
        self.call_blocking(
            req_id,
            PosOp::FillSell {
                qty,
                price,
                release_frozen,
            },
        )
    }
}
