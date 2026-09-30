//! serde deserialization must go through business validation; invalid JSON
//! must be rejected.
//! Run: cargo test -p msg-domain --features serde
#![cfg(feature = "serde")]

use std::sync::LazyLock;

use msg_domain::{
    domain_date, domain_enum, domain_flags, domain_int, domain_string, domain_time,
    domain_timestamp, DomainError,
};
use regex::Regex;
use serde::{Deserialize, Serialize};

domain_int!(Age, i64, min = 0, max = 200);
domain_int!(AnyCode, i32);

static ACCOUNT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9_]+$").unwrap());

domain_string!(Account, max_chars = 8, not_empty, pattern = &ACCOUNT_RE);

domain_enum!(
    AccountState, i32 {
        Normal        = 1 => is_normal;
        Forbidden     = 2 => is_forbidden;
        EmailValidate = 3 => is_email_validate;
    }
);

domain_enum!(
    Side, str {
        Buy  = "B" => is_buy;
        Sell = "S" => is_sell;
    }
);

domain_flags!(
    Permissions, i32 {
        READ   = 1 => can_read;
        WRITE  = 2 => can_write;
        DELETE = 4 => can_delete;
        UPDATE = 8 => can_update;
    }
);

domain_date!(TradeDate);
domain_time!(TradeTimeMs, frac = 3);
domain_timestamp!(EventTimeMs, unit = millis);
domain_timestamp!(EventTimeUs, unit = micros);

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Person {
    age: Age,
    account: Option<Account>, // nullable: null passes through; non-null is validated
    code: AnyCode,
    state: AccountState,
    side: Side,
    perms: Permissions,
    trade_date: TradeDate,
    trade_time: TradeTimeMs,
}

fn sample_json(state: i32, side: &str, perms: i32, age: i64) -> String {
    format!(
        r#"{{"age":{age},"account":"alice","code":-7,"state":{state},"side":"{side}","perms":{perms},"trade_date":20260930,"trade_time":120403333}}"#
    )
}

#[test]
fn deserialize_valid() {
    let p: Person = serde_json::from_str(&sample_json(1, "B", 7, 30)).unwrap();
    assert_eq!(p.age.get(), 30);
    assert_eq!(p.account.unwrap().as_str(), "alice");
    assert!(p.state.is_normal());
    assert!(p.side.is_buy());
    assert!(p.perms.can_read() && p.perms.can_write() && p.perms.can_delete());
    assert!(!p.perms.can_update());
    assert_eq!(p.trade_date.to_compact(), 20260930);
    assert_eq!(p.trade_date.to_iso_string(), "2026-09-30");
    assert_eq!(p.trade_time.to_hms_string(), "12:04:03.333");

    // null maps to Option::None
    let p: Person =
        serde_json::from_str(r#"{"age": 1, "account": null, "code": 0,
             "state": 2, "side": "S", "perms": 0,
             "trade_date": 20260101, "trade_time": 1}"#)
        .unwrap();
    assert!(p.account.is_none());
    assert!(p.state.is_forbidden());
    assert!(p.perms.is_empty());
}

#[test]
fn deserialize_invalid_int_rejected() {
    let err = serde_json::from_str::<Person>(
        r#"{"age": 201, "account": null, "code": 0, "state": 1, "side": "B", "perms": 0}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("Age"));
}

#[test]
fn deserialize_invalid_string_rejected() {
    let err = serde_json::from_str::<Person>(
        r#"{"age": 1, "account": "ab-c", "code": 0, "state": 1, "side": "B", "perms": 0}"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("Account"));
}

#[test]
fn deserialize_unknown_enum_code_rejected() {
    let err = serde_json::from_str::<Person>(&sample_json(99, "B", 0, 1)).unwrap_err();
    assert!(err.to_string().contains("AccountState"));
}

#[test]
fn deserialize_unknown_enum_tag_rejected() {
    let err = serde_json::from_str::<Person>(&sample_json(1, "X", 0, 1)).unwrap_err();
    assert!(err.to_string().contains("Side"));
}

#[test]
fn deserialize_unknown_flag_bits_rejected() {
    let err = serde_json::from_str::<Person>(&sample_json(1, "B", 16, 1)).unwrap_err();
    assert!(err.to_string().contains("Permissions"));
}

#[test]
fn deserialize_bad_date_rejected() {
    // 2026-02-30 is not a real day
    let err = serde_json::from_str::<Person>(&sample_json(1, "B", 0, 1).replace("20260930", "20260230"))
        .unwrap_err();
    assert!(err.to_string().contains("TradeDate"));
}

#[test]
fn deserialize_bad_time_rejected() {
    // hour 24 is invalid
    let err = serde_json::from_str::<Person>(&sample_json(1, "B", 0, 1).replace("120403333", "240000000"))
        .unwrap_err();
    assert!(err.to_string().contains("TradeTimeMs"));
}

#[test]
fn serialize_uses_wire_representation() {
    let p = Person {
        age: Age::new(42).unwrap(),
        account: Account::new("bob").ok(),
        code: AnyCode::new(9).unwrap(),
        state: AccountState::EmailValidate,
        side: Side::Sell,
        perms: Permissions::READ | Permissions::WRITE,
        trade_date: TradeDate::from_compact(20260930).unwrap(),
        trade_time: TradeTimeMs::from_packed(120403333).unwrap(),
    };
    let json = serde_json::to_string(&p).unwrap();
    // int enums/flags/date/time -> integer codes, str enum -> tag string
    assert_eq!(
        json,
        r#"{"age":42,"account":"bob","code":9,"state":3,"side":"S","perms":3,"trade_date":20260930,"trade_time":120403333}"#
    );
}

#[test]
fn timestamp_serde_uses_declared_wire_unit() {
    // 123 ms on the wire.
    let t: EventTimeMs = serde_json::from_str("123").unwrap();
    assert_eq!(t.raw(), 123);
    assert_eq!(t.epoch_micros(), 123_000);
    assert_eq!(serde_json::to_string(&t).unwrap(), "123");

    // Canonical micros remain micros.
    let u: EventTimeUs = serde_json::from_str("123000").unwrap();
    assert_eq!(u.epoch_micros(), 123_000);
    assert_eq!(serde_json::to_string(&u).unwrap(), "123000");

    // No special string syntax: JSON numbers only, matching the int8/int4 wire.
    assert!(serde_json::from_str::<EventTimeUs>("\"123000\"").is_err());
}

#[test]
fn error_is_std_error() {
    let e = Age::new(-1).unwrap_err();
    let _: &dyn std::error::Error = &e;
    assert_eq!(e, DomainError::TooSmall { ty: "Age", min: 0, value: -1 });
}
