//! Integration tests through the public shard API: no oversell under
//! concurrency, idempotent retries, weighted average / PnL, snapshot reads
//! and the event stream.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use crossbeam_channel::unbounded;
use msg_account::{AccountError, AccountEvent, AccountShard, CashAccount, PosOp, PositionAccount};

/// Monotonic request id generator shared by producer threads (0 is reserved
/// for non-idempotent commands). Stored in u64 here; production request ids
/// are normally `(producer_id, seq)` packed into u128.
struct ReqIds {
    next: AtomicU64,
}
impl ReqIds {
    fn new(start: u64) -> Self {
        Self {
            next: AtomicU64::new(start),
        }
    }
    fn get(&self) -> u128 {
        self.next.fetch_add(1, Ordering::Relaxed) as u128
    }
}

#[test]
fn cash_lifecycle_and_rejection() {
    let shard = AccountShard::spawn(0, None).unwrap();
    let acct = CashAccount::open(&shard, 1, 10_000).unwrap();

    assert_eq!(acct.freeze_blocking(1, 3_000).unwrap(), 1);
    let snap = acct.current();
    assert_eq!((snap.cash, snap.frozen), (10_000, 3_000));
    assert_eq!(snap.available(), 7_000);

    // Over-freeze rejected, version unchanged.
    let err = acct.freeze_blocking(2, 8_000).unwrap_err();
    assert_eq!(
        err,
        AccountError::InsufficientCash {
            available: 7_000,
            requested: 8_000
        }
    );
    assert_eq!(acct.current().version, 1);

    // Duplicate open is rejected.
    assert!(matches!(
        CashAccount::open(&shard, 1, 1),
        Err(AccountError::AccountExists)
    ));

    // Settle and strongly consistent read.
    acct.settle_blocking(3, 3_000, 3_000).unwrap();
    let snap = acct.snapshot_blocking().unwrap();
    assert_eq!((snap.cash, snap.frozen), (7_000, 0));

    shard.stop();
}

#[test]
fn retried_request_is_idempotent() {
    let shard = AccountShard::spawn(0, None).unwrap();
    let acct = CashAccount::open(&shard, 7, 10_000).unwrap();

    let v1 = acct.freeze_blocking(123, 1_000).unwrap();
    // Client retries after a timeout with the SAME req_id: the freeze is
    // applied exactly once and the original version is returned.
    let v2 = acct.freeze_blocking(123, 1_000).unwrap();
    assert_eq!(v1, v2);

    let snap = acct.current();
    assert_eq!(snap.frozen, 1_000);
    assert_eq!(snap.version, 1);
    shard.stop();
}

#[test]
fn concurrent_freezes_never_oversell() {
    // 100_000 cash, freeze request of 100 each, 2000 requests from many
    // threads: exactly 1000 must succeed, final state exactly exhausted.
    let shard = AccountShard::spawn(0, None).unwrap();
    let acct = Arc::new(CashAccount::open(&shard, 42, 100_000).unwrap());
    let ids = Arc::new(ReqIds::new(1_000));

    let mut handles = Vec::new();
    for _ in 0..10 {
        let acct = acct.clone();
        let ids = ids.clone();
        handles.push(thread::spawn(move || {
            let mut accepted = 0u64;
            for _ in 0..200 {
                if acct.freeze_blocking(ids.get(), 100).is_ok() {
                    accepted += 1;
                }
            }
            accepted
        }));
    }

    let accepted: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let snap = acct.current();
    assert_eq!(accepted, 1_000);
    assert_eq!(snap.frozen, 100_000);
    assert_eq!(snap.available(), 0);
    assert!(snap.frozen <= snap.cash);
    shard.stop();
}

#[test]
fn position_average_pnl_and_concurrent_no_oversell() {
    let shard = AccountShard::spawn(0, None).unwrap();
    let pos = Arc::new(PositionAccount::open(&shard, 1, 42).unwrap());

    // 100 @ 1000 then 100 @ 1100 -> weighted average 1050.
    let r = pos.fill_buy_blocking(1, 100, 1000, 0).unwrap();
    assert_eq!(r.avg_long, 1000);
    let r = pos.fill_buy_blocking(2, 100, 1100, 0).unwrap();
    assert_eq!(r.avg_long, 1050);

    // Freeze and sell 120 @ 1100 -> realized 120 * (1100 - 1050) = 6000.
    pos.freeze_sell_blocking(3, 120).unwrap();
    let r = pos.fill_sell_blocking(4, 120, 1100, 120).unwrap();
    assert_eq!(r.realized_pnl_delta, 6_000);
    assert_eq!(pos.current().long_available(), 80);

    // Duplicated fill req_id never double-counts.
    let dup = PosOp::FillSell {
        qty: 10,
        price: 1080,
        release_frozen: 0,
    };
    let a = pos.call_blocking(99, dup).unwrap();
    let b = pos.call_blocking(99, dup).unwrap();
    assert_eq!(a, b);
    assert_eq!(pos.current().long_qty, 70);
    assert_eq!(pos.current().realized_pnl, 6_300); // + 10 * (1080-1050)

    // Concurrent freezes: 70 available, demand 2000, exactly 70 accepted.
    let ids = Arc::new(ReqIds::new(5_000));
    let mut handles = Vec::new();
    for _ in 0..10 {
        let pos = pos.clone();
        let ids = ids.clone();
        handles.push(thread::spawn(move || {
            let mut accepted = 0u64;
            for _ in 0..200 {
                if pos.freeze_sell_blocking(ids.get(), 1).is_ok() {
                    accepted += 1;
                }
            }
            accepted
        }));
    }
    let accepted: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let snap = pos.current();
    assert_eq!(accepted, 70);
    assert_eq!(snap.long_frozen, 70);
    assert_eq!(snap.long_available(), 0);
    assert!(snap.long_frozen <= snap.long_qty);
    shard.stop();
}

#[test]
fn event_stream_publishes_every_mutation_once() {
    let (tx, rx) = unbounded::<AccountEvent>();
    let shard = AccountShard::spawn(0, Some(tx)).unwrap();
    let acct = CashAccount::open(&shard, 9, 10_000).unwrap();

    acct.deposit_blocking(1, 5_000).unwrap();
    acct.freeze_blocking(2, 2_000).unwrap();
    // Rejected command produces no event.
    acct.freeze_blocking(3, 1_000_000).unwrap_err();

    let mut cash_events = 0;
    while let Ok(ev) = rx.try_recv() {
        match ev {
            AccountEvent::CashChanged(s) => {
                cash_events += 1;
                assert!(s.version >= 1);
            }
            AccountEvent::PositionChanged(_) => panic!("unexpected position event"),
        }
    }
    assert_eq!(cash_events, 2); // deposit + freeze, rejection excluded
    shard.stop();
}
