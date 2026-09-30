//! msg-domain: domain newtypes with business constraints.
//!
//! Rules:
//! 1. Validity is guaranteed only at construction (`new`/`TryFrom`/sqlx
//!    Decode/serde Deserialize); the inner field is private.
//! 2. `From<inner>` is intentionally NOT implemented (it would bypass checks).
//! 3. PG Decode never trusts the database: dirty rows are rejected at the edge.
//! 4. Nullability is always expressed by an outer `Option<T>`.
//!
//! ```
//! use msg_domain::{domain_int, domain_string};
//!
//! domain_int!(pub Age, i64, min = 0, max = 200);
//! assert!(Age::new(201).is_err());
//!
//! domain_string!(pub Account, min_chars = 1, max_chars = 8);
//! assert!(Account::new("").is_err());
//! ```

use std::error::Error;
use std::fmt;

#[cfg(feature = "serde")]
pub use serde;
#[cfg(feature = "sqlx")]
pub use sqlx;

/// Shared validation error for all domain types.
///
/// Edge adapters (gRPC/FIX/REST) map this to protocol-specific error codes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainError {
    /// Value below the lower bound
    TooSmall { ty: &'static str, min: i128, value: i128 },
    /// Value above the upper bound
    TooLarge { ty: &'static str, max: i128, value: i128 },
    /// Empty string not allowed
    Empty { ty: &'static str },
    /// Character count below the lower bound
    TooShort { ty: &'static str, min_chars: usize, value: usize },
    /// Character count above the upper bound
    TooLong { ty: &'static str, max_chars: usize, value: usize },
    /// Regex pattern mismatch
    Regex { ty: &'static str },
    /// Unknown integer code for a state enum
    UnknownCode { ty: &'static str, value: i128 },
    /// Unknown string tag for a state enum
    UnknownTag { ty: &'static str, value: String },
    /// Bit flags contain bits outside the declared mask
    UnknownBits { ty: &'static str, value: i128, valid_mask: i128 },
    /// Invalid compact date (not a real calendar day)
    BadDate { ty: &'static str, value: i32 },
    /// Invalid compact time (hour/minute/second/fraction out of range)
    BadTime { ty: &'static str, value: i64 },
    /// Exchange has no fixed UTC offset (DST observed); a session calendar
    /// must supply the offset for the concrete day.
    NoFixedOffset { exchange: &'static str },
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DomainError::TooSmall { ty, min, value } => {
                write!(f, "{ty} must be >= {min}, got {value}")
            }
            DomainError::TooLarge { ty, max, value } => {
                write!(f, "{ty} must be <= {max}, got {value}")
            }
            DomainError::Empty { ty } => write!(f, "{ty} must not be empty"),
            DomainError::TooShort { ty, min_chars, value } => {
                write!(f, "{ty} length must be >= {min_chars} chars, got {value}")
            }
            DomainError::TooLong { ty, max_chars, value } => {
                write!(f, "{ty} length must be <= {max_chars} chars, got {value}")
            }
            DomainError::Regex { ty } => write!(f, "{ty} does not match required pattern"),
            DomainError::UnknownCode { ty, value } => {
                write!(f, "{ty} got unknown code {value}")
            }
            DomainError::UnknownTag { ty, value } => {
                write!(f, "{ty} got unknown tag {value:?}")
            }
            DomainError::UnknownBits { ty, value, valid_mask } => {
                write!(f, "{ty} bits {value} contain undefined bits (mask = {valid_mask})")
            }
            DomainError::BadDate { ty, value } => {
                write!(f, "{ty} invalid compact date {value} (expected YYYYMMDD)")
            }
            DomainError::BadTime { ty, value } => {
                write!(f, "{ty} invalid compact time {value} (expected HHMMSS with fraction)")
            }
            DomainError::NoFixedOffset { exchange } => {
                write!(
                    f,
                    "{exchange} observes DST and has no fixed UTC offset; supply a session-calendar offset"
                )
            }
        }
    }
}

impl Error for DomainError {}

// ---------------------------------------------------------------------------
// Calendar / clock helpers (zero dependencies)
// ---------------------------------------------------------------------------

#[doc(hidden)]
pub mod cal {
    /// Field order after the 4-digit year in a compact/separated date.
    ///
    /// `Ymd` = year-month-day (`20260930` / `2026/9/30`),
    /// `Ydm` = year-day-month (`20260309` / `2026/3/9`, both Sep 3rd).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum DateOrder {
        Ymd,
        Ydm,
    }

    /// Const-comparable equality (derived `PartialEq` is not const).
    #[inline]
    pub const fn same(a: DateOrder, b: DateOrder) -> bool {
        matches!(a, DateOrder::Ymd) == matches!(b, DateOrder::Ymd)
    }

    /// Split a packed `YYYYxxxx` integer into `(year, month, day)` following
    /// the declared field order.
    #[inline]
    pub const fn split_compact(v: i32, order: DateOrder) -> (i32, u8, u8) {
        let year = v / 10000;
        let a = ((v / 100) % 100) as u8;
        let b = (v % 100) as u8;
        match order {
            DateOrder::Ymd => (year, a, b),
            DateOrder::Ydm => (year, b, a),
        }
    }

    /// Pack `(year, month, day)` into the compact integer for `order`.
    #[inline]
    pub const fn pack_ymd(year: i32, month: u8, day: u8, order: DateOrder) -> i32 {
        let (m, d) = match order {
            DateOrder::Ymd => (month, day),
            DateOrder::Ydm => (day, month),
        };
        year * 10000 + m as i32 * 100 + d as i32
    }

    /// Gregorian leap year.
    #[inline]
    pub const fn is_leap(year: i32) -> bool {
        year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
    }

    /// Days in a month; returns 0 for a bad month.
    #[inline]
    pub const fn days_in_month(year: i32, month: u8) -> u8 {
        const DAYS: [u8; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        match month {
            1..=12 => {
                let d = DAYS[month as usize - 1];
                if month == 2 && is_leap(year) {
                    29
                } else {
                    d
                }
            }
            _ => 0,
        }
    }

    /// Validate a Gregorian date in the financial range 1900..=9999.
    #[inline]
    pub const fn valid_ymd(year: i32, month: u8, day: u8) -> bool {
        year >= 1900
            && year <= 9999
            && month >= 1
            && month <= 12
            && day >= 1
            && day <= days_in_month(year, month)
    }

    /// Day of week via Sakamoto's algorithm: 0 = Sunday ... 6 = Saturday.
    #[inline]
    pub const fn weekday(year: i32, month: u8, day: u8) -> u8 {
        const T: [i64; 12] = [0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
        let mut y = year as i64;
        if month < 3 {
            y -= 1;
        }
        let w = (y + y / 4 - y / 100 + y / 400 + T[month as usize - 1] + day as i64) % 7;
        w as u8
    }

    /// Days since 1970-01-01 (Howard Hinnant's civil-from-days algorithm).
    #[inline]
    pub const fn days_from_civil(year: i32, month: u8, day: u8) -> i64 {
        let y = if month <= 2 {
            year as i64 - 1
        } else {
            year as i64
        };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = y - era * 400;
        let m = month as i64;
        let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day as i64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era * 146097 + doe - 719468
    }

    /// Inverse of [`days_from_civil`]: returns `(year, month, day)`.
    #[inline]
    pub const fn civil_from_days(z: i64) -> (i32, u8, u8) {
        let z2 = z + 719468;
        let era = if z2 >= 0 { z2 } else { z2 - 146096 } / 146097;
        let doe = z2 - era * 146097;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = (doy - (153 * mp + 2) / 5 + 1) as u8;
        let m = if mp < 10 { mp + 3 } else { mp - 9 } as u8;
        let year = (y + if m <= 2 { 1 } else { 0 }) as i32;
        (year, m, d)
    }

    /// 10^n for the small fixed fraction scales.
    #[inline]
    pub const fn pow10(n: u32) -> i64 {
        let mut r = 1i64;
        let mut i = 0u32;
        while i < n {
            r *= 10;
            i += 1;
        }
        r
    }

    /// Validate H/M/S and a sub-second fraction with `scale` decimal digits.
    ///
    /// Second 60 is accepted only as the positive leap-second slot 23:59:60.
    #[inline]
    pub const fn valid_hms_frac(h: u8, m: u8, s: u8, frac: i64, scale: u32) -> bool {
        let clock_ok = h <= 23
            && m <= 59
            && (s <= 59 || (h == 23 && m == 59 && s == 60));
        clock_ok && frac >= 0 && frac < pow10(scale)
    }

    /// Break an epoch-microsecond instant into exchange wall-clock parts.
    ///
    /// `offset_min_east` is the exchange's fixed offset east of UTC in
    /// minutes (Shanghai = 480, Tokyo = 540). Euclidean division keeps the
    /// day boundary correct for pre-1970 / negative instants.
    ///
    /// Returns `(year, month, day, hour, minute, second, microsecond)`.
    #[inline]
    pub const fn epoch_parts_us(
        epoch_us: i64,
        offset_min_east: i32,
    ) -> (i32, u8, u8, u8, u8, u8, i64) {
        const US_PER_SEC: i64 = 1_000_000;
        const US_PER_DAY: i64 = 86_400 * US_PER_SEC;

        let shifted = epoch_us + offset_min_east as i64 * 60 * US_PER_SEC;
        let day_no = shifted.div_euclid(US_PER_DAY);
        let tod_us = shifted.rem_euclid(US_PER_DAY);
        let (y, m, d) = civil_from_days(day_no);

        let secs = tod_us / US_PER_SEC;
        let frac = tod_us % US_PER_SEC;
        (
            y,
            m,
            d,
            (secs / 3600) as u8,
            ((secs / 60) % 60) as u8,
            (secs % 60) as u8,
            frac,
        )
    }
}

// ---------------------------------------------------------------------------
// Time units and exchanges
// ---------------------------------------------------------------------------

/// Resolution of a timestamp on the wire / in a message field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    /// Epoch milliseconds (i64), FIX legacy precision.
    Millis,
    /// Epoch microseconds (i64), the platform's canonical internal precision.
    Micros,
    /// Epoch nanoseconds (i64), PTP / hardware-timestamp environments.
    Nanos,
}

impl TimeUnit {
    #[inline]
    pub const fn name(self) -> &'static str {
        match self {
            TimeUnit::Millis => "millis",
            TimeUnit::Micros => "micros",
            TimeUnit::Nanos => "nanos",
        }
    }

    /// Convert a raw value in this unit to canonical epoch microseconds.
    #[inline]
    pub const fn to_micros(self, raw: i64) -> i64 {
        match self {
            TimeUnit::Millis => raw * 1_000,
            TimeUnit::Micros => raw,
            // Floor so negative instants keep a consistent day boundary.
            TimeUnit::Nanos => raw.div_euclid(1_000),
        }
    }

    /// Convert canonical epoch microseconds to a raw value in this unit.
    #[inline]
    pub const fn from_micros(self, epoch_us: i64) -> i64 {
        match self {
            TimeUnit::Millis => epoch_us.div_euclid(1_000),
            TimeUnit::Micros => epoch_us,
            TimeUnit::Nanos => epoch_us * 1_000,
        }
    }
}

/// Trading venue whose wall clock determines the local trade date/time.
///
/// Offsets are minutes east of UTC. DST venues intentionally return `None`
/// from [`Exchange::fixed_offset_minutes`]: their offset depends on the date,
/// so a session calendar must resolve it rather than baking a wrong constant
/// into the type system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Exchange {
    /// UTC reference clock (no exchange).
    Utc,
    /// Shanghai Stock Exchange / Shanghai Futures (China Standard Time).
    Sse,
    /// Shenzhen Stock Exchange.
    Szse,
    /// Hong Kong Exchanges and Clearing (HKT, no DST since 1979).
    Hkex,
    /// Japan Exchange Group / Tokyo Stock Exchange (JST, no DST).
    Tse,
    /// Korea Exchange (KST, no DST).
    Krx,
    /// Singapore Exchange (SGT, no DST after 1981).
    Sgx,
    /// London Stock Exchange (GMT/BST, observes DST).
    Lse,
    /// New York Stock Exchange (EST/EDT, observes DST).
    Nyse,
    /// Nasdaq (EST/EDT, observes DST).
    Nasdaq,
    /// CME Group (CST/CDT for most US futures, observes DST).
    Cme,
    /// Intercontinental Exchange (observes DST).
    Ice,
}

impl Exchange {
    /// Stable machine code used in configs and protocol fields.
    #[inline]
    pub const fn code(self) -> &'static str {
        match self {
            Exchange::Utc => "UTC",
            Exchange::Sse => "SSE",
            Exchange::Szse => "SZSE",
            Exchange::Hkex => "HKEX",
            Exchange::Tse => "TSE",
            Exchange::Krx => "KRX",
            Exchange::Sgx => "SGX",
            Exchange::Lse => "LSE",
            Exchange::Nyse => "NYSE",
            Exchange::Nasdaq => "NASDAQ",
            Exchange::Cme => "CME",
            Exchange::Ice => "ICE",
        }
    }

    /// Fixed offset in minutes east of UTC; `None` for DST venues.
    #[inline]
    pub const fn fixed_offset_minutes(self) -> Option<i32> {
        match self {
            Exchange::Utc => Some(0),
            Exchange::Sse | Exchange::Szse | Exchange::Hkex | Exchange::Sgx => Some(8 * 60),
            Exchange::Tse | Exchange::Krx => Some(9 * 60),
            Exchange::Lse | Exchange::Nyse | Exchange::Nasdaq | Exchange::Cme | Exchange::Ice => {
                None
            }
        }
    }

    #[inline]
    pub const fn observes_dst(self) -> bool {
        self.fixed_offset_minutes().is_none()
    }
}

/// Calendar/clock parts of an instant at an exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExchangeParts {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    /// Microsecond within the second, always `0..1_000_000`.
    pub microsecond: i64,
}

impl ExchangeParts {
    /// Packed `HHMMSS` in the exchange wall clock.
    #[inline]
    pub const fn hhmmss(self) -> i32 {
        self.hour as i32 * 10000 + self.minute as i32 * 100 + self.second as i32
    }
}

// ---------------------------------------------------------------------------
// Internal helper macros
// ---------------------------------------------------------------------------

pub use cal::DateOrder;

/// Normalize an optional min/max expression into `Option<T>`.
#[macro_export]
#[doc(hidden)]
macro_rules! __domain_opt {
    () => {
        ::core::option::Option::None
    };
    ($v:expr) => {
        ::core::option::Option::Some($v)
    };
}

/// Internal implementation of [`domain_string!`].
///
/// Bracket groups carry the optional constraints; `$ne` is the resolved
/// `not_empty` flag. Keeping all generated code here avoids duplicating it
/// across the two public matcher arms.
#[macro_export]
#[doc(hidden)]
macro_rules! __domain_string_impl {
    (
        $vis:vis $name:ident
        [$($minc:expr)?]
        [$($maxc:expr)?]
        $ne:literal
        [$($re:expr)?]
    ) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        $vis struct $name(::std::string::String);

        impl $name {
            /// The only constructor.
            #[inline]
            pub fn new(
                s: impl ::core::convert::Into<::std::string::String>,
            ) -> ::core::result::Result<Self, $crate::DomainError> {
                let s = s.into();
                if $ne && s.is_empty() {
                    return ::core::result::Result::Err($crate::DomainError::Empty {
                        ty: ::core::stringify!($name),
                    });
                }
                let n = s.chars().count();
                $(
                    if n < $minc {
                        return ::core::result::Result::Err($crate::DomainError::TooShort {
                            ty: ::core::stringify!($name),
                            min_chars: $minc,
                            value: n,
                        });
                    }
                )?
                $(
                    if n > $maxc {
                        return ::core::result::Result::Err($crate::DomainError::TooLong {
                            ty: ::core::stringify!($name),
                            max_chars: $maxc,
                            value: n,
                        });
                    }
                )?
                $(
                    if !$re.is_match(&s) {
                        return ::core::result::Result::Err($crate::DomainError::Regex {
                            ty: ::core::stringify!($name),
                        });
                    }
                )?
                ::core::result::Result::Ok(Self(s))
            }

            #[inline]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            #[inline]
            pub fn into_inner(self) -> ::std::string::String {
                self.0
            }

            /// Length in characters (not bytes).
            #[inline]
            pub fn len_chars(&self) -> usize {
                self.0.chars().count()
            }
        }

        impl ::core::convert::TryFrom<::std::string::String> for $name {
            type Error = $crate::DomainError;

            #[inline]
            fn try_from(
                s: ::std::string::String,
            ) -> ::core::result::Result<Self, Self::Error> {
                Self::new(s)
            }
        }

        impl ::core::convert::AsRef<str> for $name {
            #[inline]
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(&self.0)
            }
        }

        $crate::__domain_serde_string!($name);
        $crate::__domain_sqlx_string!($name);
    };
}

// ---- serde integration (optional feature) ----

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "serde")]
macro_rules! __domain_serde_int {
    ($name:ident, $inner:ty) => {
        impl $crate::serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
            where
                S: $crate::serde::Serializer,
            {
                <$inner as $crate::serde::Serialize>::serialize(&self.0, serializer)
            }
        }

        impl<'de> $crate::serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
            where
                D: $crate::serde::Deserializer<'de>,
            {
                let raw = <$inner as $crate::serde::Deserialize>::deserialize(deserializer)?;
                $name::new(raw).map_err($crate::serde::de::Error::custom)
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "serde"))]
macro_rules! __domain_serde_int {
    ($name:ident, $inner:ty) => {};
}

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "serde")]
macro_rules! __domain_serde_string {
    ($name:ident) => {
        impl $crate::serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
            where
                S: $crate::serde::Serializer,
            {
                <::std::string::String as $crate::serde::Serialize>::serialize(&self.0, serializer)
            }
        }

        impl<'de> $crate::serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
            where
                D: $crate::serde::Deserializer<'de>,
            {
                let raw = <::std::string::String as $crate::serde::Deserialize>::deserialize(
                    deserializer,
                )?;
                $name::new(raw).map_err($crate::serde::de::Error::custom)
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "serde"))]
macro_rules! __domain_serde_string {
    ($name:ident) => {};
}

// ---- sqlx/Postgres integration (optional feature; Decode goes through new()) ----

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "sqlx")]
macro_rules! __domain_sqlx_int {
    ($name:ident, $inner:ty) => {
        impl<'q> $crate::sqlx::Encode<'q, $crate::sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut <$crate::sqlx::Postgres as $crate::sqlx::Database>::ArgumentBuffer<'q>,
            ) -> ::core::result::Result<
                $crate::sqlx::encode::IsNull,
                $crate::sqlx::error::BoxDynError,
            > {
                <$inner as $crate::sqlx::Encode<'q, $crate::sqlx::Postgres>>::encode_by_ref(
                    &self.0,
                    buf,
                )
            }
        }

        impl<'r> $crate::sqlx::Decode<'r, $crate::sqlx::Postgres> for $name {
            fn decode(
                value: <$crate::sqlx::Postgres as $crate::sqlx::Database>::ValueRef<'r>,
            ) -> ::core::result::Result<Self, $crate::sqlx::error::BoxDynError> {
                let raw =
                    <$inner as $crate::sqlx::Decode<'r, $crate::sqlx::Postgres>>::decode(value)?;
                // Never trust the DB: historical dirty rows are rejected here.
                ::core::result::Result::Ok($name::new(raw)?)
            }
        }

        impl $crate::sqlx::Type<$crate::sqlx::Postgres> for $name {
            fn type_info() -> <$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo {
                <$inner as $crate::sqlx::Type<$crate::sqlx::Postgres>>::type_info()
            }

            fn compatible(
                ty: &<$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo,
            ) -> bool {
                <$inner as $crate::sqlx::Type<$crate::sqlx::Postgres>>::compatible(ty)
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "sqlx"))]
macro_rules! __domain_sqlx_int {
    ($name:ident, $inner:ty) => {};
}

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "sqlx")]
macro_rules! __domain_sqlx_string {
    ($name:ident) => {
        impl<'q> $crate::sqlx::Encode<'q, $crate::sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut <$crate::sqlx::Postgres as $crate::sqlx::Database>::ArgumentBuffer<'q>,
            ) -> ::core::result::Result<
                $crate::sqlx::encode::IsNull,
                $crate::sqlx::error::BoxDynError,
            > {
                <::std::string::String as $crate::sqlx::Encode<
                    'q,
                    $crate::sqlx::Postgres,
                >>::encode_by_ref(&self.0, buf)
            }
        }

        impl<'r> $crate::sqlx::Decode<'r, $crate::sqlx::Postgres> for $name {
            fn decode(
                value: <$crate::sqlx::Postgres as $crate::sqlx::Database>::ValueRef<'r>,
            ) -> ::core::result::Result<Self, $crate::sqlx::error::BoxDynError> {
                let raw = <::std::string::String as $crate::sqlx::Decode<
                    'r,
                    $crate::sqlx::Postgres,
                >>::decode(value)?;
                ::core::result::Result::Ok($name::new(raw)?)
            }
        }

        impl $crate::sqlx::Type<$crate::sqlx::Postgres> for $name {
            fn type_info() -> <$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo {
                <::std::string::String as $crate::sqlx::Type<$crate::sqlx::Postgres>>::type_info()
            }

            fn compatible(
                ty: &<$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo,
            ) -> bool {
                <::std::string::String as $crate::sqlx::Type<$crate::sqlx::Postgres>>::compatible(
                    ty,
                )
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "sqlx"))]
macro_rules! __domain_sqlx_string {
    ($name:ident) => {};
}

// ---------------------------------------------------------------------------
// Public macros
// ---------------------------------------------------------------------------

/// Define a bounded integer domain type.
///
/// `min` / `max` are both optional, covering: unbounded / lower-only /
/// upper-only / both-bounded. Missing branches vanish at macro expansion time.
///
/// # Examples
///
/// ```
/// use msg_domain::domain_int;
///
/// domain_int!(pub AnyI64,   i64);                      // unbounded
/// domain_int!(pub NonNeg,    i64, min = 0);            // lower only
/// domain_int!(pub RetryCnt,  i32, max = 10);           // upper only
/// domain_int!(pub Age,       i64, min = 0, max = 200); // both
/// ```
#[macro_export]
macro_rules! domain_int {
    (
        $vis:vis $name:ident, $inner:ty
        $(, min = $min:expr)?
        $(, max = $max:expr)?
        $(,)?
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        $vis struct $name($inner);

        impl $name {
            /// Lower bound (`None` means unbounded).
            pub const MIN: ::core::option::Option<$inner> =
                $crate::__domain_opt!($($min)?);
            /// Upper bound (`None` means unbounded).
            pub const MAX: ::core::option::Option<$inner> =
                $crate::__domain_opt!($($max)?);

            /// The only constructor: invalid values cannot enter the type.
            #[inline]
            pub fn new(v: $inner) -> ::core::result::Result<Self, $crate::DomainError> {
                $(
                    if v < $min {
                        return ::core::result::Result::Err($crate::DomainError::TooSmall {
                            ty: ::core::stringify!($name),
                            min: $min as i128,
                            value: v as i128,
                        });
                    }
                )?
                $(
                    if v > $max {
                        return ::core::result::Result::Err($crate::DomainError::TooLarge {
                            ty: ::core::stringify!($name),
                            max: $max as i128,
                            value: v as i128,
                        });
                    }
                )?
                ::core::result::Result::Ok(Self(v))
            }

            /// Read the raw inner value.
            #[inline(always)]
            pub fn get(self) -> $inner {
                self.0
            }
        }

        impl ::core::convert::TryFrom<$inner> for $name {
            type Error = $crate::DomainError;

            #[inline]
            fn try_from(v: $inner) -> ::core::result::Result<Self, Self::Error> {
                Self::new(v)
            }
        }

        // From<$inner> / DerefMut are intentionally NOT implemented.

        $crate::__domain_serde_int!($name, $inner);
        $crate::__domain_sqlx_int!($name, $inner);
    };
}

/// Define a bounded string domain type.
///
/// Three orthogonal, all-optional dimensions:
/// - `min_chars` / `max_chars`: length in **characters** (not bytes; matches
///   PG `varchar(n)`);
/// - `not_empty`: reject the empty string (equivalent to `min_chars = 1` but
///   with a clearer error);
/// - `pattern`: regex applied to non-empty values (pass a precompiled
///   `&'static regex::Regex`).
///
/// Nullable columns are expressed as `Option<T>` on the entity side; no
/// second macro is needed.
///
/// # Examples
///
/// ```
/// use std::sync::LazyLock;
/// use regex::Regex;
/// use msg_domain::domain_string;
///
/// static ACCOUNT_RE: LazyLock<Regex> =
///     LazyLock::new(|| Regex::new(r"^[A-Za-z0-9_]+$").unwrap());
///
/// domain_string!(pub Remark,    max_chars = 255);                        // length only
/// domain_string!(pub Account,   max_chars = 8, not_empty, pattern = &ACCOUNT_RE);
/// // entity field: pub broker: Option<BrokerCode>
/// domain_string!(pub BrokerCode, max_chars = 16);
/// ```
/// Argument order is fixed: `min_chars`, `max_chars`, `not_empty`, `pattern`.
#[macro_export]
macro_rules! domain_string {
    // Arm 1: with the `not_empty` flag (must follow min/max, precede pattern).
    (
        $vis:vis $name:ident
        $(, min_chars = $minc:expr)?
        $(, max_chars = $maxc:expr)?
        , not_empty
        $(, pattern = $re:expr)?
        $(,)?
    ) => {
        $crate::__domain_string_impl!(
            $vis $name
            [$($minc)?]
            [$($maxc)?]
            true
            [$($re)?]
        );
    };
    // Arm 2: without `not_empty`.
    (
        $vis:vis $name:ident
        $(, min_chars = $minc:expr)?
        $(, max_chars = $maxc:expr)?
        $(, pattern = $re:expr)?
        $(,)?
    ) => {
        $crate::__domain_string_impl!(
            $vis $name
            [$($minc)?]
            [$($maxc)?]
            false
            [$($re)?]
        );
    };
}

// ---------------------------------------------------------------------------
// State enums and bit flags
// ---------------------------------------------------------------------------

// ---- serde integration ----

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "serde")]
macro_rules! __domain_serde_enum_int {
    ($name:ident, $ity:ident) => {
        impl $crate::serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
            where
                S: $crate::serde::Serializer,
            {
                <$ity as $crate::serde::Serialize>::serialize(&self.code(), serializer)
            }
        }

        impl<'de> $crate::serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
            where
                D: $crate::serde::Deserializer<'de>,
            {
                let code = <$ity as $crate::serde::Deserialize>::deserialize(deserializer)?;
                $name::from_code(code).map_err($crate::serde::de::Error::custom)
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "serde"))]
macro_rules! __domain_serde_enum_int {
    ($name:ident, $ity:ident) => {};
}

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "serde")]
macro_rules! __domain_serde_enum_str {
    ($name:ident) => {
        impl $crate::serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
            where
                S: $crate::serde::Serializer,
            {
                serializer.serialize_str(self.tag())
            }
        }

        impl<'de> $crate::serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
            where
                D: $crate::serde::Deserializer<'de>,
            {
                let tag = <::std::string::String as $crate::serde::Deserialize>::deserialize(
                    deserializer,
                )?;
                $name::from_tag(&tag).map_err($crate::serde::de::Error::custom)
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "serde"))]
macro_rules! __domain_serde_enum_str {
    ($name:ident) => {};
}

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "serde")]
macro_rules! __domain_serde_flags {
    ($name:ident, $ity:ident) => {
        impl $crate::serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
            where
                S: $crate::serde::Serializer,
            {
                <$ity as $crate::serde::Serialize>::serialize(&self.bits(), serializer)
            }
        }

        impl<'de> $crate::serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
            where
                D: $crate::serde::Deserializer<'de>,
            {
                let bits = <$ity as $crate::serde::Deserialize>::deserialize(deserializer)?;
                $name::from_bits(bits).map_err($crate::serde::de::Error::custom)
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "serde"))]
macro_rules! __domain_serde_flags {
    ($name:ident, $ity:ident) => {};
}

// ---- sqlx/Postgres integration ----

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "sqlx")]
macro_rules! __domain_sqlx_enum_int {
    ($name:ident, $ity:ident) => {
        impl<'q> $crate::sqlx::Encode<'q, $crate::sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut <$crate::sqlx::Postgres as $crate::sqlx::Database>::ArgumentBuffer<'q>,
            ) -> ::core::result::Result<
                $crate::sqlx::encode::IsNull,
                $crate::sqlx::error::BoxDynError,
            > {
                <$ity as $crate::sqlx::Encode<'q, $crate::sqlx::Postgres>>::encode_by_ref(
                    &self.code(),
                    buf,
                )
            }
        }

        impl<'r> $crate::sqlx::Decode<'r, $crate::sqlx::Postgres> for $name {
            fn decode(
                value: <$crate::sqlx::Postgres as $crate::sqlx::Database>::ValueRef<'r>,
            ) -> ::core::result::Result<Self, $crate::sqlx::error::BoxDynError> {
                let code =
                    <$ity as $crate::sqlx::Decode<'r, $crate::sqlx::Postgres>>::decode(value)?;
                ::core::result::Result::Ok($name::from_code(code)?)
            }
        }

        impl $crate::sqlx::Type<$crate::sqlx::Postgres> for $name {
            fn type_info() -> <$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo {
                <$ity as $crate::sqlx::Type<$crate::sqlx::Postgres>>::type_info()
            }

            fn compatible(
                ty: &<$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo,
            ) -> bool {
                <$ity as $crate::sqlx::Type<$crate::sqlx::Postgres>>::compatible(ty)
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "sqlx"))]
macro_rules! __domain_sqlx_enum_int {
    ($name:ident, $ity:ident) => {};
}

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "sqlx")]
macro_rules! __domain_sqlx_enum_str {
    ($name:ident) => {
        impl<'q> $crate::sqlx::Encode<'q, $crate::sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut <$crate::sqlx::Postgres as $crate::sqlx::Database>::ArgumentBuffer<'q>,
            ) -> ::core::result::Result<
                $crate::sqlx::encode::IsNull,
                $crate::sqlx::error::BoxDynError,
            > {
                <&str as $crate::sqlx::Encode<'q, $crate::sqlx::Postgres>>::encode(
                    self.tag(),
                    buf,
                )
            }
        }

        impl<'r> $crate::sqlx::Decode<'r, $crate::sqlx::Postgres> for $name {
            fn decode(
                value: <$crate::sqlx::Postgres as $crate::sqlx::Database>::ValueRef<'r>,
            ) -> ::core::result::Result<Self, $crate::sqlx::error::BoxDynError> {
                let tag = <::std::string::String as $crate::sqlx::Decode<
                    'r,
                    $crate::sqlx::Postgres,
                >>::decode(value)?;
                ::core::result::Result::Ok($name::from_tag(&tag)?)
            }
        }

        impl $crate::sqlx::Type<$crate::sqlx::Postgres> for $name {
            fn type_info() -> <$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo {
                <::std::string::String as $crate::sqlx::Type<$crate::sqlx::Postgres>>::type_info()
            }

            fn compatible(
                ty: &<$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo,
            ) -> bool {
                <::std::string::String as $crate::sqlx::Type<$crate::sqlx::Postgres>>::compatible(
                    ty,
                )
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "sqlx"))]
macro_rules! __domain_sqlx_enum_str {
    ($name:ident) => {};
}

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "sqlx")]
macro_rules! __domain_sqlx_flags {
    ($name:ident, $ity:ident) => {
        impl<'q> $crate::sqlx::Encode<'q, $crate::sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut <$crate::sqlx::Postgres as $crate::sqlx::Database>::ArgumentBuffer<'q>,
            ) -> ::core::result::Result<
                $crate::sqlx::encode::IsNull,
                $crate::sqlx::error::BoxDynError,
            > {
                <$ity as $crate::sqlx::Encode<'q, $crate::sqlx::Postgres>>::encode_by_ref(
                    &self.bits(),
                    buf,
                )
            }
        }

        impl<'r> $crate::sqlx::Decode<'r, $crate::sqlx::Postgres> for $name {
            fn decode(
                value: <$crate::sqlx::Postgres as $crate::sqlx::Database>::ValueRef<'r>,
            ) -> ::core::result::Result<Self, $crate::sqlx::error::BoxDynError> {
                let bits =
                    <$ity as $crate::sqlx::Decode<'r, $crate::sqlx::Postgres>>::decode(value)?;
                ::core::result::Result::Ok($name::from_bits(bits)?)
            }
        }

        impl $crate::sqlx::Type<$crate::sqlx::Postgres> for $name {
            fn type_info() -> <$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo {
                <$ity as $crate::sqlx::Type<$crate::sqlx::Postgres>>::type_info()
            }

            fn compatible(
                ty: &<$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo,
            ) -> bool {
                <$ity as $crate::sqlx::Type<$crate::sqlx::Postgres>>::compatible(ty)
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "sqlx"))]
macro_rules! __domain_sqlx_flags {
    ($name:ident, $ity:ident) => {};
}

/// Define a mutually-exclusive state enum (exactly one state at a time).
///
/// Two carriers are supported: an integer type (`i32`/`i64`, stored as PG
/// `int4`/`int8`) or `str` (stored as PG `text`/`varchar`).
///
/// Each variant declares its wire/db representation and a domain predicate
/// name (e.g. `is_normal`), so call sites read `state.is_normal()` instead
/// of comparing magic numbers.
///
/// # Examples
///
/// ```
/// use msg_domain::domain_enum;
///
/// domain_enum!(
///     pub AccountState, i32 {
///        Normal        = 1 => is_normal;
///        Forbidden     = 2 => is_forbidden;
///        EmailValidate = 3 => is_email_validate;
///     }
/// );
///
/// assert!(AccountState::from_code(1).unwrap().is_normal());
/// assert!(AccountState::from_code(99).is_err());
///
/// domain_enum!(
///     pub Side, str {
///        Buy  = "B" => is_buy;
///        Sell = "S" => is_sell;
///     }
/// );
/// assert!(Side::from_tag("B").unwrap().is_buy());
/// ```
#[macro_export]
macro_rules! domain_enum {
    // String carrier. Must precede the integer arm: `str` is also an ident.
    (
        $vis:vis $name:ident, str {
            $($v:ident = $tag:literal => $pred:ident);+ $(;)?
        }
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        $vis enum $name {
            $($v,)+
        }

        impl $name {
            /// All declared variants in declaration order.
            pub const ALL: &'static [$name] = &[$($name::$v,)+];

            /// Wire/database string tag.
            #[inline]
            pub const fn tag(self) -> &'static str {
                match self {
                    $(Self::$v => $tag,)+
                }
            }

            /// Parse from a wire/database tag; unknown tags are rejected.
            #[inline]
            pub fn from_tag(tag: &str) -> ::core::result::Result<Self, $crate::DomainError> {
                match tag {
                    $($tag => ::core::result::Result::Ok(Self::$v),)+
                    other => ::core::result::Result::Err($crate::DomainError::UnknownTag {
                        ty: ::core::stringify!($name),
                        value: other.to_owned(),
                    }),
                }
            }

            /// Rust variant name (for logs/metrics).
            #[inline]
            pub const fn name(self) -> &'static str {
                match self {
                    $(Self::$v => ::core::stringify!($v),)+
                }
            }

            $(
                /// Domain predicate for this variant.
                #[inline]
                pub const fn $pred(self) -> bool {
                    ::core::matches!(self, Self::$v)
                }
            )+
        }

        impl ::core::convert::TryFrom<&str> for $name {
            type Error = $crate::DomainError;

            #[inline]
            fn try_from(tag: &str) -> ::core::result::Result<Self, Self::Error> {
                Self::from_tag(tag)
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(self.tag())
            }
        }

        $crate::__domain_serde_enum_str!($name);
        $crate::__domain_sqlx_enum_str!($name);
    };

    // Integer carrier.
    (
        $vis:vis $name:ident, $ity:ident {
            $($v:ident = $code:literal => $pred:ident);+ $(;)?
        }
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr($ity)]
        $vis enum $name {
            $($v = $code as $ity,)+
        }

        impl $name {
            /// All declared variants in declaration order.
            pub const ALL: &'static [$name] = &[$($name::$v,)+];

            /// Wire/database integer code.
            #[inline]
            pub const fn code(self) -> $ity {
                self as $ity
            }

            /// Parse from a wire/database code; unknown codes are rejected.
            #[inline]
            pub fn from_code(code: $ity) -> ::core::result::Result<Self, $crate::DomainError> {
                match code {
                    $($code => ::core::result::Result::Ok(Self::$v),)+
                    other => ::core::result::Result::Err($crate::DomainError::UnknownCode {
                        ty: ::core::stringify!($name),
                        value: other as i128,
                    }),
                }
            }

            /// Rust variant name (for logs/metrics).
            #[inline]
            pub const fn name(self) -> &'static str {
                match self {
                    $(Self::$v => ::core::stringify!($v),)+
                }
            }

            $(
                /// Domain predicate for this variant.
                #[inline]
                pub const fn $pred(self) -> bool {
                    ::core::matches!(self, Self::$v)
                }
            )+
        }

        impl ::core::convert::TryFrom<$ity> for $name {
            type Error = $crate::DomainError;

            #[inline]
            fn try_from(code: $ity) -> ::core::result::Result<Self, Self::Error> {
                Self::from_code(code)
            }
        }

        $crate::__domain_serde_enum_int!($name, $ity);
        $crate::__domain_sqlx_enum_int!($name, $ity);
    };
}

/// Whitelist the storage types allowed by [`domain_flags!`].
///
/// Only signed 32/64-bit integers map cleanly to PG `int4`/`int8`; any other
/// token hits the fallback arm and fails compilation.
#[macro_export]
#[doc(hidden)]
macro_rules! __domain_flags_ty {
    (i32) => {
        i32
    };
    (i64) => {
        i64
    };
    ($other:ty) => {
        ::core::compile_error!("domain_flags! storage type must be i32 or i64 (PG int4/int8)")
    };
}

/// Entry syntax: each entry is either `FLAG = value => predicate` (explicit
/// power-of-two value) or `FLAG @ index => predicate` (`1 << index`). The two
/// forms may be mixed. This macro normalizes both into one accumulator.
#[macro_export]
#[doc(hidden)]
macro_rules! __domain_flags_parse {
    // Explicit value form.
    (
        $vis:vis $name:ident, $ity:ident,
        [$(($flag:ident, $value:expr, $pred:ident))*]
        $next_flag:ident = $bit:literal => $next_pred:ident ; $($rest:tt)*
    ) => {
        $crate::__domain_flags_parse!(
            $vis $name, $ity,
            [$(($flag, $value, $pred))* ($next_flag, ($bit), $next_pred)]
            $($rest)*
        );
    };
    // Index form: FLAG @ k == 1 << k.
    (
        $vis:vis $name:ident, $ity:ident,
        [$(($flag:ident, $value:expr, $pred:ident))*]
        $next_flag:ident @ $idx:literal => $next_pred:ident ; $($rest:tt)*
    ) => {
        $crate::__domain_flags_parse!(
            $vis $name, $ity,
            [$(($flag, $value, $pred))* ($next_flag, (1i64 << $idx), $next_pred)]
            $($rest)*
        );
    };
    // Optional trailing semicolon.
    ($vis:vis $name:ident, $ity:ident, [$($entries:tt)*] ;) => {
        $crate::__domain_flags_parse!($vis $name, $ity, [$($entries)*]);
    };
    // Terminator -> code generation.
    ($vis:vis $name:ident, $ity:ident, [$(($flag:ident, $value:expr, $pred:ident))*]) => {
        $crate::__domain_flags_impl!($vis $name, $ity, [$(($flag, $value, $pred))*]);
    };
}

/// Generated code for [`domain_flags!`]. Kept separate from the parser so the
/// two syntax forms share one implementation.
#[macro_export]
#[doc(hidden)]
macro_rules! __domain_flags_impl {
    (
        $vis:vis $name:ident, $ity:ident,
        [$(($flag:ident, $value:expr, $pred:ident))*]
    ) => {
        // Compile-time gate + validation of every declared bit.
        // The type alias only exists inside this block, so multiple
        // domain_flags! invocations in one module never collide.
        const _: () = {
            #[allow(dead_code)]
            type __AllowedFlagsTy = $crate::__domain_flags_ty!($ity);

            // positive, power of two, fits the storage type, no duplicates.
            const fn validate(values: &[i64]) {
                let max = <$ity>::MAX as i64;
                let mut i = 0usize;
                while i < values.len() {
                    let b = values[i];
                    assert!(
                        b > 0,
                        "domain_flags!: bit value must be positive (use @index form for 1 << k)"
                    );
                    assert!(
                        b & (b - 1) == 0,
                        "domain_flags!: value must be a power of two (1, 2, 4, 8, ...)"
                    );
                    assert!(
                        b <= max,
                        "domain_flags!: bit exceeds the storage type maximum"
                    );
                    let mut j = i + 1;
                    while j < values.len() {
                        assert!(values[j] != b, "domain_flags!: duplicate bit value");
                        j += 1;
                    }
                    i += 1;
                }
            }
            validate(&[$($value as i64),*]);
        };

        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        $vis struct $name($ity);

        impl $name {
            /// No permissions.
            pub const EMPTY: Self = Self(0);

            $(
                /// Single-bit permission constant.
                pub const $flag: Self = Self(($value) as $ity);
            )*

            const MASK_VALUE: $ity = 0 $(| (($value) as $ity))*;
            /// Union of every declared bit.
            pub const MASK: Self = Self(Self::MASK_VALUE);

            /// Build from a raw bit pattern; undefined bits are rejected.
            #[inline]
            pub fn from_bits(
                bits: $ity,
            ) -> ::core::result::Result<Self, $crate::DomainError> {
                if bits & !Self::MASK_VALUE != 0 {
                    return ::core::result::Result::Err($crate::DomainError::UnknownBits {
                        ty: ::core::stringify!($name),
                        value: bits as i128,
                        valid_mask: Self::MASK_VALUE as i128,
                    });
                }
                ::core::result::Result::Ok(Self(bits))
            }

            /// Raw bit pattern.
            #[inline]
            pub const fn bits(self) -> $ity {
                self.0
            }

            /// No bit set.
            #[inline]
            pub const fn is_empty(self) -> bool {
                self.0 == 0
            }

            /// True iff every bit of `other` is present.
            #[inline]
            pub const fn contains(self, other: Self) -> bool {
                self.0 & other.0 == other.0
            }

            /// Union of two permission sets.
            #[inline]
            pub const fn union(self, other: Self) -> Self {
                Self(self.0 | other.0)
            }

            /// Intersection of two permission sets.
            #[inline]
            pub const fn intersection(self, other: Self) -> Self {
                Self(self.0 & other.0)
            }

            /// Bits in `self` but not in `other`.
            #[inline]
            pub const fn difference(self, other: Self) -> Self {
                Self(self.0 & !other.0)
            }

            /// Toggle the given bits.
            #[inline]
            pub const fn toggle(self, other: Self) -> Self {
                Self(self.0 ^ other.0)
            }

            /// Add permissions in place.
            #[inline]
            pub fn insert(&mut self, other: Self) {
                self.0 |= other.0;
            }

            /// Remove permissions in place.
            #[inline]
            pub fn remove(&mut self, other: Self) {
                self.0 &= !other.0;
            }

            $(
                /// Domain predicate: check one permission without bit math.
                #[inline]
                pub const fn $pred(self) -> bool {
                    self.0 & (($value) as $ity) != 0
                }
            )*
        }

        impl ::core::convert::TryFrom<$ity> for $name {
            type Error = $crate::DomainError;

            #[inline]
            fn try_from(bits: $ity) -> ::core::result::Result<Self, Self::Error> {
                Self::from_bits(bits)
            }
        }

        impl ::core::ops::BitOr for $name {
            type Output = Self;
            #[inline]
            fn bitor(self, rhs: Self) -> Self {
                self.union(rhs)
            }
        }

        impl ::core::ops::BitOrAssign for $name {
            #[inline]
            fn bitor_assign(&mut self, rhs: Self) {
                self.insert(rhs);
            }
        }

        impl ::core::ops::BitAnd for $name {
            type Output = Self;
            #[inline]
            fn bitand(self, rhs: Self) -> Self {
                self.intersection(rhs)
            }
        }

        impl ::core::ops::BitAndAssign for $name {
            #[inline]
            fn bitand_assign(&mut self, rhs: Self) {
                self.0 = self.0 & rhs.0;
            }
        }

        impl ::core::ops::BitXor for $name {
            type Output = Self;
            #[inline]
            fn bitxor(self, rhs: Self) -> Self {
                self.toggle(rhs)
            }
        }

        impl ::core::ops::BitXorAssign for $name {
            #[inline]
            fn bitxor_assign(&mut self, rhs: Self) {
                self.0 = self.0 ^ rhs.0;
            }
        }

        impl ::core::ops::Sub for $name {
            type Output = Self;
            #[inline]
            fn sub(self, rhs: Self) -> Self {
                self.difference(rhs)
            }
        }

        impl ::core::ops::SubAssign for $name {
            #[inline]
            fn sub_assign(&mut self, rhs: Self) {
                self.remove(rhs);
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                ::core::write!(f, "{}", self.0)
            }
        }

        $crate::__domain_serde_flags!($name, $ity);
        $crate::__domain_sqlx_flags!($name, $ity);
    };
}

/// Define a bit-flag permission set with domain predicate methods.
///
/// Storage must be `i32` or `i64` (PG `int4`/`int8`); any other type is a
/// compile error. Every entry is one unique bit; it may be declared either as
/// an explicit power of two (`READ = 1`) or by bit index (`READ @ 0`, meaning
/// `1 << 0`). The two forms may be mixed. Invalid values (not a power of two,
/// duplicates, out of type range) are **compile-time errors**.
///
/// Business code uses domain methods (`can_read`) and set operations
/// (`|`, `-`, `contains`) instead of raw bit math. `from_bits` rejects any
/// undefined bit, so dirty DB/JSON values carrying reserved bits are stopped
/// at the edge.
///
/// # Examples
///
/// ```
/// use msg_domain::domain_flags;
///
/// domain_flags!(
///     pub Permissions, i32 {
///        READ   = 1 => can_read;
///        WRITE  = 2 => can_write;
///        DELETE = 4 => can_delete;
///        UPDATE = 8 => can_update;
///     }
/// );
///
/// let p = Permissions::from_bits(7).unwrap(); // READ | WRITE | DELETE
/// assert!(p.can_read() && p.can_write() && p.can_delete());
/// assert!(!p.can_update());
/// assert_eq!((Permissions::READ | Permissions::WRITE).bits(), 3);
///
/// // Index form (1 << k); the two forms are equivalent and may be mixed.
/// domain_flags!(
///     IdxPerms, i64 {
///        READ @ 0 => can_read;   // 1
///        WRITE @ 1 => can_write; // 2
///     }
/// );
/// assert_eq!(IdxPerms::WRITE.bits(), 2);
/// ```
#[macro_export]
macro_rules! domain_flags {
    (
        $vis:vis $name:ident, $ity:ident { $($body:tt)* }
    ) => {
        $crate::__domain_flags_parse!($vis $name, $ity, [] $($body)*);
    };
}

// ---- serde/sqlx for date/time: integer-like but a named constructor ----

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "serde")]
macro_rules! __domain_serde_int_ctor {
    ($name:ident, $ity:ident, $ctor:ident) => {
        impl $crate::serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
            where
                S: $crate::serde::Serializer,
            {
                <$ity as $crate::serde::Serialize>::serialize(&self.0, serializer)
            }
        }

        impl<'de> $crate::serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
            where
                D: $crate::serde::Deserializer<'de>,
            {
                let raw = <$ity as $crate::serde::Deserialize>::deserialize(deserializer)?;
                $name::$ctor(raw).map_err($crate::serde::de::Error::custom)
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "serde"))]
macro_rules! __domain_serde_int_ctor {
    ($name:ident, $ity:ident, $ctor:ident) => {};
}

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "sqlx")]
macro_rules! __domain_sqlx_int_ctor {
    ($name:ident, $ity:ident, $ctor:ident) => {
        impl<'q> $crate::sqlx::Encode<'q, $crate::sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut <$crate::sqlx::Postgres as $crate::sqlx::Database>::ArgumentBuffer<'q>,
            ) -> ::core::result::Result<
                $crate::sqlx::encode::IsNull,
                $crate::sqlx::error::BoxDynError,
            > {
                <$ity as $crate::sqlx::Encode<'q, $crate::sqlx::Postgres>>::encode_by_ref(
                    &self.0,
                    buf,
                )
            }
        }

        impl<'r> $crate::sqlx::Decode<'r, $crate::sqlx::Postgres> for $name {
            fn decode(
                value: <$crate::sqlx::Postgres as $crate::sqlx::Database>::ValueRef<'r>,
            ) -> ::core::result::Result<Self, $crate::sqlx::error::BoxDynError> {
                let raw =
                    <$ity as $crate::sqlx::Decode<'r, $crate::sqlx::Postgres>>::decode(value)?;
                ::core::result::Result::Ok($name::$ctor(raw)?)
            }
        }

        impl $crate::sqlx::Type<$crate::sqlx::Postgres> for $name {
            fn type_info() -> <$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo {
                <$ity as $crate::sqlx::Type<$crate::sqlx::Postgres>>::type_info()
            }

            fn compatible(
                ty: &<$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo,
            ) -> bool {
                <$ity as $crate::sqlx::Type<$crate::sqlx::Postgres>>::compatible(ty)
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "sqlx"))]
macro_rules! __domain_sqlx_int_ctor {
    ($name:ident, $ity:ident, $ctor:ident) => {};
}

/// Compact calendar date stored in a signed 32-bit integer (PG `int4`).
///
/// The field order is declared on the type:
/// - `domain_date!(pub TradeDate)` or `order = ymd`: `YYYYMMDD` (default)
/// - `domain_date!(pub D, order = ydm)`: `YYYYDDMM`
///
/// Parsing accepts the compact 8-digit form, or separated forms with `-` or
/// `/` and optional zero padding (`"2026-9-3"`, `"2026/09/03"`), always in
/// the type's declared order. Use `parse_fmt` to state the order explicitly
/// per call when the source is ambiguous.
///
/// # Examples
///
/// ```
/// use msg_domain::{domain_date, DateOrder};
///
/// domain_date!(pub TradeDate);                 // default YYYYMMDD
///
/// let d = TradeDate::from_compact(20260930).unwrap();
/// assert_eq!((d.year(), d.month(), d.day()), (2026, 9, 30));
/// assert_eq!(d.to_iso_string(), "2026-09-30");
/// assert_eq!(TradeDate::parse("2026-09-30").unwrap(), d);
/// assert_eq!(TradeDate::parse("2026/9/30").unwrap(), d);   // slash, no padding
/// assert!(TradeDate::from_compact(20260230).is_err());     // not a real day
/// assert_eq!(d.add_days(1).to_compact(), 20261001);
///
/// // Ambiguous source: declare the order at the call site.
/// assert_eq!(
///     TradeDate::parse_fmt("2026/9/3", DateOrder::Ymd).unwrap(), // Sep 3
///     TradeDate::from_ymd(2026, 9, 3).unwrap()
/// );
/// assert_eq!(
///     TradeDate::parse_fmt("2026/3/9", DateOrder::Ydm).unwrap(), // Sep 3 too
///     TradeDate::from_ymd(2026, 9, 3).unwrap()
/// );
///
/// // A type whose wire format is YYYYDDMM.
/// domain_date!(pub EuroDate, order = ydm);
/// let e = EuroDate::from_ymd(2026, 9, 3).unwrap();
/// assert_eq!(e.to_compact(), 20260309);
/// assert_eq!(EuroDate::parse("2026/3/9").unwrap(), e);
/// ```
#[macro_export]
macro_rules! domain_date {
    ($vis:vis $name:ident) => {
        $crate::__domain_date_impl!($vis $name, Ymd);
    };
    ($vis:vis $name:ident, order = ymd) => {
        $crate::__domain_date_impl!($vis $name, Ymd);
    };
    ($vis:vis $name:ident, order = ydm) => {
        $crate::__domain_date_impl!($vis $name, Ydm);
    };
    ($vis:vis $name:ident, order = $other:ident) => {
        ::core::compile_error!("domain_date! order must be ymd or ydm")
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __domain_date_impl {
    ($vis:vis $name:ident, $order:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        $vis struct $name(i32);

        impl $name {
            /// Field order of this type's compact representation.
            pub const ORDER: $crate::DateOrder = $crate::DateOrder::$order;

            /// Build from year/month/day; non-existent calendar days are rejected.
            #[inline]
            pub const fn from_ymd(
                year: i32,
                month: u8,
                day: u8,
            ) -> ::core::result::Result<Self, $crate::DomainError> {
                if !$crate::cal::valid_ymd(year, month, day) {
                    return ::core::result::Result::Err($crate::DomainError::BadDate {
                        ty: ::core::stringify!($name),
                        value: $crate::cal::pack_ymd(year, month, day, Self::ORDER),
                    });
                }
                ::core::result::Result::Ok(Self($crate::cal::pack_ymd(
                    year,
                    month,
                    day,
                    Self::ORDER,
                )))
            }

            /// Build from a compact integer following [`Self::ORDER`].
            #[inline]
            pub const fn from_compact(v: i32) -> ::core::result::Result<Self, $crate::DomainError> {
                Self::from_compact_fmt(v, Self::ORDER)
            }

            /// Build from a compact integer whose digits follow `order`.
            #[inline]
            pub const fn from_compact_fmt(
                v: i32,
                order: $crate::DateOrder,
            ) -> ::core::result::Result<Self, $crate::DomainError> {
                let (year, month, day) = $crate::cal::split_compact(v, order);
                if !$crate::cal::valid_ymd(year, month, day) {
                    return ::core::result::Result::Err($crate::DomainError::BadDate {
                        ty: ::core::stringify!($name),
                        value: v,
                    });
                }
                // Stored canonically in the type's own declared order.
                if $crate::cal::same(order, Self::ORDER) {
                    ::core::result::Result::Ok(Self(v))
                } else {
                    Self::from_ymd(year, month, day)
                }
            }

            /// Parse in the type's declared order: `20260930`, `2026-09-30`,
            /// `2026/9/3` (separator `-` or `/`, zero padding optional).
            #[inline]
            pub fn parse(s: &str) -> ::core::result::Result<Self, $crate::DomainError> {
                Self::parse_fmt(s, Self::ORDER)
            }

            /// Parse with an explicitly stated field order.
            pub fn parse_fmt(
                s: &str,
                order: $crate::DateOrder,
            ) -> ::core::result::Result<Self, $crate::DomainError> {
                fn err() -> $crate::DomainError {
                    $crate::DomainError::BadDate {
                        ty: ::core::stringify!($name),
                        value: 0,
                    }
                }
                fn num(p: &str) -> ::core::result::Result<u32, $crate::DomainError> {
                    if p.is_empty() || !p.bytes().all(|c| c.is_ascii_digit()) {
                        return ::core::result::Result::Err(err());
                    }
                    p.parse::<u32>().map_err(|_| err())
                }

                let s = s.trim();
                let b = s.as_bytes();

                // Compact 8 digits: YYYYxxxx.
                if b.len() == 8 && b.iter().all(|c| c.is_ascii_digit()) {
                    let v: i32 = s.parse().map_err(|_| err())?;
                    return Self::from_compact_fmt(v, order);
                }

                // Separated form: one separator kind, exactly 3 fields.
                let sep = if b.contains(&b'/') {
                    b'/'
                } else if b.contains(&b'-') {
                    b'-'
                } else {
                    return ::core::result::Result::Err(err());
                };
                // Mixed separators are rejected.
                let other = if sep == b'/' { b'-' } else { b'/' };
                if b.contains(&other) {
                    return ::core::result::Result::Err(err());
                }
                let parts: ::std::vec::Vec<&str> = s.split(sep as char).collect();
                if parts.len() != 3 {
                    return ::core::result::Result::Err(err());
                }
                let y = num(parts[0])?;
                let x = num(parts[1])?;
                let z = num(parts[2])?;
                if !(1900..=9999).contains(&y) || x < 1 || x > 99 || z < 1 || z > 99 {
                    return ::core::result::Result::Err(err());
                }
                let (month, day) = match order {
                    $crate::DateOrder::Ymd => (x as u8, z as u8),
                    $crate::DateOrder::Ydm => (z as u8, x as u8),
                };
                Self::from_ymd(y as i32, month, day)
            }

            /// Compact integer in the type's declared order (wire/DB representation).
            #[inline(always)]
            pub const fn to_compact(self) -> i32 {
                self.0
            }

            #[inline]
            pub const fn year(self) -> i32 {
                $crate::cal::split_compact(self.0, Self::ORDER).0
            }
            #[inline]
            pub const fn month(self) -> u8 {
                $crate::cal::split_compact(self.0, Self::ORDER).1
            }
            #[inline]
            pub const fn day(self) -> u8 {
                $crate::cal::split_compact(self.0, Self::ORDER).2
            }

            /// Canonical ISO string, always `YYYY-MM-DD` regardless of wire order.
            pub fn to_iso_string(self) -> ::std::string::String {
                ::std::format!(
                    "{:04}-{:02}-{:02}",
                    self.year(),
                    self.month(),
                    self.day()
                )
            }

            /// 0 = Monday ... 6 = Sunday (business convention).
            #[inline]
            pub const fn weekday_monday0(self) -> u8 {
                // cal::weekday is 0 = Sunday ... 6 = Saturday.
                ($crate::cal::weekday(self.year(), self.month(), self.day()) + 6) % 7
            }

            /// 0 = Sunday ... 6 = Saturday.
            #[inline]
            pub const fn weekday(self) -> u8 {
                $crate::cal::weekday(self.year(), self.month(), self.day())
            }

            #[inline]
            pub const fn is_weekend(self) -> bool {
                let w = $crate::cal::weekday(self.year(), self.month(), self.day());
                w == 0 || w == 6
            }

            /// Date arithmetic: T+N / T-N calendar days, over real Gregorian months.
            #[inline]
            pub const fn add_days(self, delta: i64) -> Self {
                let z = $crate::cal::days_from_civil(
                    self.year(),
                    self.month(),
                    self.day(),
                ) + delta;
                let (y, m, d) = $crate::cal::civil_from_days(z);
                Self($crate::cal::pack_ymd(y, m, d, Self::ORDER))
            }

            /// Whole calendar days between two dates (`self - other`).
            #[inline]
            pub const fn diff_days(self, other: Self) -> i64 {
                $crate::cal::days_from_civil(self.year(), self.month(), self.day())
                    - $crate::cal::days_from_civil(other.year(), other.month(), other.day())
            }
        }

        impl ::core::convert::TryFrom<i32> for $name {
            type Error = $crate::DomainError;
            #[inline]
            fn try_from(v: i32) -> ::core::result::Result<Self, Self::Error> {
                Self::from_compact(v)
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(&self.to_iso_string())
            }
        }

        $crate::__domain_serde_int_ctor!($name, i32, from_compact);
        $crate::__domain_sqlx_int_ctor!($name, i32, from_compact);
    };
}

/// Compact wall-clock time.
///
/// Stored as `HHMMSS` (fraction scale 0, i32, PG `int4`), `HHMMSSmmm`
/// (scale 3, milliseconds, i32) or `HHMMSSuuuuuu` (scale 6, microseconds,
/// i64, PG `int8`). The fraction scale is fixed at compile time.
///
/// # Examples
///
/// ```
/// use msg_domain::domain_time;
///
/// domain_time!(pub TimeSec);            // 120403
/// domain_time!(pub TimeMs, frac = 3);   // 120403333
/// domain_time!(pub TimeUs, frac = 6);   // 120403333000
///
/// let t = TimeMs::from_packed(120403333).unwrap();
/// assert_eq!(t.hour(), 12);
/// assert_eq!(t.minute(), 4);
/// assert_eq!(t.second(), 3);
/// assert_eq!(t.frac(), 333);
/// assert_eq!(t.to_hms_string(), "12:04:03.333");
/// assert_eq!(TimeMs::parse("12:04:03.333").unwrap(), t);
/// assert!(TimeMs::from_packed(126003000).is_err()); // minute 60
/// ```
#[macro_export]
macro_rules! domain_time {
    ($vis:vis $name:ident) => {
        $crate::__domain_time_impl!($vis $name, 0u32, i32);
    };
    ($vis:vis $name:ident, frac = 0) => {
        $crate::__domain_time_impl!($vis $name, 0u32, i32);
    };
    ($vis:vis $name:ident, frac = 3) => {
        $crate::__domain_time_impl!($vis $name, 3u32, i32);
    };
    ($vis:vis $name:ident, frac = 6) => {
        $crate::__domain_time_impl!($vis $name, 6u32, i64);
    };
    ($vis:vis $name:ident, frac = $f:literal) => {
        ::core::compile_error!("domain_time! supports only frac = 0, 3 or 6");
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __domain_time_impl {
    ($vis:vis $name:ident, $scale:expr, $ity:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        $vis struct $name($ity);

        impl $name {
            /// Fractional digits: 0 = seconds, 3 = milliseconds, 6 = microseconds.
            pub const FRAC_SCALE: u32 = $scale;
            const BASE: i64 = $crate::cal::pow10($scale);

            /// Build from hour/minute/second and a sub-second fraction.
            #[inline]
            pub const fn from_hms_frac(
                hour: u8,
                minute: u8,
                second: u8,
                frac: i64,
            ) -> ::core::result::Result<Self, $crate::DomainError> {
                if !$crate::cal::valid_hms_frac(hour, minute, second, frac, $scale) {
                    return ::core::result::Result::Err($crate::DomainError::BadTime {
                        ty: ::core::stringify!($name),
                        value: -1,
                    });
                }
                let hms = hour as i64 * 10000 + minute as i64 * 100 + second as i64;
                ::core::result::Result::Ok(Self((hms * Self::BASE + frac) as $ity))
            }

            /// Build from hour/minute/second (fraction zero).
            #[inline]
            pub const fn from_hms(
                hour: u8,
                minute: u8,
                second: u8,
            ) -> ::core::result::Result<Self, $crate::DomainError> {
                Self::from_hms_frac(hour, minute, second, 0)
            }

            /// Build from the packed wire/DB integer (`120403` / `120403333`).
            #[inline]
            pub const fn from_packed(v: $ity) -> ::core::result::Result<Self, $crate::DomainError> {
                let sv = v as i64;
                if sv < 0 {
                    return ::core::result::Result::Err($crate::DomainError::BadTime {
                        ty: ::core::stringify!($name),
                        value: sv,
                    });
                }
                let hms = sv / Self::BASE;
                let frac = sv % Self::BASE;
                let hour = (hms / 10000) as u8;
                let minute = ((hms / 100) % 100) as u8;
                let second = (hms % 100) as u8;
                if !$crate::cal::valid_hms_frac(hour, minute, second, frac, $scale) {
                    return ::core::result::Result::Err($crate::DomainError::BadTime {
                        ty: ::core::stringify!($name),
                        value: sv,
                    });
                }
                ::core::result::Result::Ok(Self(v))
            }

            /// Parse `"12:04:03"` / `"12:04:03.333"` or the compact `"120403[.333]"`.
            pub fn parse(s: &str) -> ::core::result::Result<Self, $crate::DomainError> {
                fn bad() -> $crate::DomainError {
                    $crate::DomainError::BadTime {
                        ty: ::core::stringify!($name),
                        value: -1,
                    }
                }
                fn two(b: &[u8], i: usize) -> ::core::result::Result<u8, $crate::DomainError> {
                    if !b[i].is_ascii_digit() || !b[i + 1].is_ascii_digit() {
                        return ::core::result::Result::Err(bad());
                    }
                    Ok((b[i] - b'0') * 10 + (b[i + 1] - b'0'))
                }

                let s = s.trim();
                let (hms_part, frac_part): (&str, Option<&str>) = match s.split_once('.') {
                    Some((a, b)) => (a, Some(b)),
                    None => (s, None),
                };

                let (h, m, sec) = if hms_part.contains(':') {
                    let b = hms_part.as_bytes();
                    if b.len() != 8 || b[2] != b':' || b[5] != b':' {
                        return ::core::result::Result::Err(bad());
                    }
                    (two(b, 0)?, two(b, 3)?, two(b, 6)?)
                } else {
                    // Compact digits HHMMSS.
                    let v: i64 = hms_part
                        .parse()
                        .map_err(|_| bad())?;
                    ((v / 10000) as u8, ((v / 100) % 100) as u8, (v % 100) as u8)
                };

                let frac: i64 = match frac_part {
                    None => 0,
                    Some(f) => {
                        if f.is_empty() || !f.bytes().all(|c| c.is_ascii_digit()) {
                            return ::core::result::Result::Err(bad());
                        }
                        let scale = $scale as usize;
                        if f.len() == scale {
                            f.parse().unwrap_or(0)
                        } else if f.len() < scale {
                            // Pad on the right: ".3" == ".300" at scale 3.
                            f.parse::<i64>().map_err(|_| bad())?
                                * $crate::cal::pow10((scale - f.len()) as u32)
                        } else {
                            // More digits than the scale: trailing digits must all be 0.
                            let (head, tail) = f.split_at(scale);
                            if tail.bytes().any(|c| c != b'0') {
                                return ::core::result::Result::Err(bad());
                            }
                            if scale == 0 {
                                0
                            } else {
                                head.parse().map_err(|_| bad())?
                            }
                        }
                    }
                };

                Self::from_hms_frac(h, m, sec, frac)
            }

            /// Packed integer (also the wire/DB representation).
            #[inline(always)]
            pub const fn to_packed(self) -> $ity {
                self.0
            }

            #[inline]
            pub const fn hour(self) -> u8 {
                ((self.0 as i64 / Self::BASE / 10000) % 100) as u8
            }
            #[inline]
            pub const fn minute(self) -> u8 {
                ((self.0 as i64 / Self::BASE / 100) % 100) as u8
            }
            #[inline]
            pub const fn second(self) -> u8 {
                (self.0 as i64 / Self::BASE % 100) as u8
            }
            /// Sub-second part in the declared scale (ms or us); 0 at scale 0.
            #[inline]
            pub const fn frac(self) -> i64 {
                self.0 as i64 % Self::BASE
            }

            /// `12:04:03` or `12:04:03.333` according to the fraction scale.
            pub fn to_hms_string(self) -> ::std::string::String {
                let frac = self.frac();
                if $scale == 0 {
                    ::std::format!(
                        "{:02}:{:02}:{:02}",
                        self.hour(),
                        self.minute(),
                        self.second()
                    )
                } else {
                    ::std::format!(
                        "{:02}:{:02}:{:02}.{:0width$}",
                        self.hour(),
                        self.minute(),
                        self.second(),
                        frac,
                        width = $scale as usize
                    )
                }
            }
        }

        impl ::core::convert::TryFrom<$ity> for $name {
            type Error = $crate::DomainError;
            #[inline]
            fn try_from(v: $ity) -> ::core::result::Result<Self, Self::Error> {
                Self::from_packed(v)
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(&self.to_hms_string())
            }
        }

        $crate::__domain_serde_int_ctor!($name, $ity, from_packed);
        $crate::__domain_sqlx_int_ctor!($name, $ity, from_packed);
    };
}

// ---- timestamp: serde uses wire unit, sqlx always uses canonical micros ----

#[macro_export]
#[doc(hidden)]
macro_rules! __ts_unit {
    (millis) => {
        $crate::TimeUnit::Millis
    };
    (micros) => {
        $crate::TimeUnit::Micros
    };
    (nanos) => {
        $crate::TimeUnit::Nanos
    };
    ($other:ident) => {
        ::core::compile_error!("domain_timestamp! unit must be millis, micros or nanos")
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __ts_exchange_opt {
    () => {
        ::core::option::Option::None
    };
    (utc) => {
        ::core::option::Option::Some($crate::Exchange::Utc)
    };
    (sse) => {
        ::core::option::Option::Some($crate::Exchange::Sse)
    };
    (szse) => {
        ::core::option::Option::Some($crate::Exchange::Szse)
    };
    (hkex) => {
        ::core::option::Option::Some($crate::Exchange::Hkex)
    };
    (tse) => {
        ::core::option::Option::Some($crate::Exchange::Tse)
    };
    (krx) => {
        ::core::option::Option::Some($crate::Exchange::Krx)
    };
    (sgx) => {
        ::core::option::Option::Some($crate::Exchange::Sgx)
    };
    (lse) => {
        ::core::option::Option::Some($crate::Exchange::Lse)
    };
    (nyse) => {
        ::core::option::Option::Some($crate::Exchange::Nyse)
    };
    (nasdaq) => {
        ::core::option::Option::Some($crate::Exchange::Nasdaq)
    };
    (cme) => {
        ::core::option::Option::Some($crate::Exchange::Cme)
    };
    (ice) => {
        ::core::option::Option::Some($crate::Exchange::Ice)
    };
    ($other:ident) => {
        ::core::compile_error!(
            "unknown exchange; supported: utc, sse, szse, hkex, tse, krx, sgx, lse, nyse, nasdaq, cme, ice"
        )
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "serde")]
macro_rules! __domain_serde_ts {
    ($name:ident) => {
        impl $crate::serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
            where
                S: $crate::serde::Serializer,
            {
                <i64 as $crate::serde::Serialize>::serialize(&self.raw(), serializer)
            }
        }

        impl<'de> $crate::serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
            where
                D: $crate::serde::Deserializer<'de>,
            {
                let raw = <i64 as $crate::serde::Deserialize>::deserialize(deserializer)?;
                ::core::result::Result::Ok(Self::from_raw(raw))
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "serde"))]
macro_rules! __domain_serde_ts {
    ($name:ident) => {};
}

#[macro_export]
#[doc(hidden)]
#[cfg(feature = "sqlx")]
macro_rules! __domain_sqlx_ts {
    ($name:ident) => {
        impl<'q> $crate::sqlx::Encode<'q, $crate::sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut <$crate::sqlx::Postgres as $crate::sqlx::Database>::ArgumentBuffer<'q>,
            ) -> ::core::result::Result<
                $crate::sqlx::encode::IsNull,
                $crate::sqlx::error::BoxDynError,
            > {
                // PG int8 column stores canonical epoch microseconds regardless
                // of the wire unit.
                <i64 as $crate::sqlx::Encode<'q, $crate::sqlx::Postgres>>::encode_by_ref(
                    &self.epoch_micros(),
                    buf,
                )
            }
        }

        impl<'r> $crate::sqlx::Decode<'r, $crate::sqlx::Postgres> for $name {
            fn decode(
                value: <$crate::sqlx::Postgres as $crate::sqlx::Database>::ValueRef<'r>,
            ) -> ::core::result::Result<Self, $crate::sqlx::error::BoxDynError> {
                let us = <i64 as $crate::sqlx::Decode<'r, $crate::sqlx::Postgres>>::decode(value)?;
                ::core::result::Result::Ok(Self::from_epoch_micros(us))
            }
        }

        impl $crate::sqlx::Type<$crate::sqlx::Postgres> for $name {
            fn type_info() -> <$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo {
                <i64 as $crate::sqlx::Type<$crate::sqlx::Postgres>>::type_info()
            }

            fn compatible(
                ty: &<$crate::sqlx::Postgres as $crate::sqlx::Database>::TypeInfo,
            ) -> bool {
                <i64 as $crate::sqlx::Type<$crate::sqlx::Postgres>>::compatible(ty)
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
#[cfg(not(feature = "sqlx"))]
macro_rules! __domain_sqlx_ts {
    ($name:ident) => {};
}

/// A timezone-absolute UTC instant.
///
/// Internally the value is always canonical **epoch microseconds**; `unit`
/// only selects the wire/serde representation (`millis`, `micros` or
/// `nanos`). The optional `exchange` parameter sets the default venue for
/// [`parts`], while `parts_at` / `parts_at_offset` allow any venue or explicit
/// offset per call.
///
/// DST exchanges (London, New York, CME, ...) have no fixed offset: `parts_at`
/// returns [`DomainError::NoFixedOffset`]; feed the session-calendar offset to
/// `parts_at_offset`. Night-session trade-date ownership is a separate session
/// calendar concern and is deliberately not encoded here.
///
/// # Examples
///
/// ```
/// use msg_domain::{domain_timestamp, Exchange};
///
/// domain_timestamp!(pub EventTime, unit = micros);
/// domain_timestamp!(pub ShEventTime, unit = millis, exchange = sse);
///
/// // Unix epoch itself: 1970-01-01 00:00:00 UTC = 08:00 in Shanghai.
/// let t = EventTime::from_epoch_micros(0);
/// let p = t.parts_at(Exchange::Sse).unwrap();
/// assert_eq!((p.year, p.month, p.day, p.hour), (1970, 1, 1, 8));
///
/// // The millis wire type serializes / parses milliseconds but keeps micros
/// // canonical internally.
/// let sh = ShEventTime::from_raw(123); // 123 ms
/// assert_eq!(sh.epoch_micros(), 123_000);
/// assert_eq!(sh.raw(), 123);
/// assert_eq!(sh.parts().unwrap().hour, 8); // default exchange = SSE
///
/// // London observes DST, so a fixed offset cannot be assumed.
/// assert!(EventTime::from_epoch_micros(0).parts_at(Exchange::Lse).is_err());
/// ```
///
/// [`parts`]: macro.domain_timestamp.html
#[macro_export]
macro_rules! domain_timestamp {
    (
        $vis:vis $name:ident, unit = $u:ident $(, exchange = $ex:ident)? $(,)?
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        $vis struct $name(i64);

        impl $name {
            /// Wire resolution of this timestamp.
            pub const UNIT: $crate::TimeUnit = $crate::__ts_unit!($u);
            /// Default venue for [`parts`], if declared on the type.
            pub const DEFAULT_EXCHANGE: ::core::option::Option<$crate::Exchange> =
                $crate::__ts_exchange_opt!($($ex)?);

            /// Construct from a raw value in the type's declared wire unit.
            #[inline]
            pub const fn from_raw(raw: i64) -> Self {
                Self(Self::UNIT.to_micros(raw))
            }

            /// Construct from canonical epoch microseconds (UTC).
            #[inline]
            pub const fn from_epoch_micros(epoch_us: i64) -> Self {
                Self(epoch_us)
            }

            /// Current wall-clock time in UTC. Inject a clock at boundaries
            /// instead of calling this in deterministic core logic.
            #[inline]
            pub fn now_utc() -> Self {
                let us = ::std::time::SystemTime::now()
                    .duration_since(::std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros().min(i64::MAX as u128) as i64)
                    .unwrap_or(0);
                Self(us)
            }

            /// Raw value in the declared wire unit.
            #[inline]
            pub const fn raw(self) -> i64 {
                Self::UNIT.from_micros(self.0)
            }

            /// Canonical epoch microseconds (UTC), independent of wire unit.
            #[inline(always)]
            pub const fn epoch_micros(self) -> i64 {
                self.0
            }

            /// Split into exchange parts using an explicit UTC offset
            /// (minutes east); use for DST venues after the session calendar
            /// resolves the day's offset.
            #[inline]
            pub const fn parts_at_offset(self, offset_min_east: i32) -> $crate::ExchangeParts {
                let (year, month, day, hour, minute, second, microsecond) =
                    $crate::cal::epoch_parts_us(self.0, offset_min_east);
                $crate::ExchangeParts {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    microsecond,
                }
            }

            /// Split into parts at a fixed-offset exchange.
            #[inline]
            pub const fn parts_at(
                self,
                exchange: $crate::Exchange,
            ) -> ::core::result::Result<$crate::ExchangeParts, $crate::DomainError> {
                match exchange.fixed_offset_minutes() {
                    ::core::option::Option::Some(offset) => {
                        ::core::result::Result::Ok(self.parts_at_offset(offset))
                    }
                    ::core::option::Option::None => {
                        ::core::result::Result::Err($crate::DomainError::NoFixedOffset {
                            exchange: exchange.code(),
                        })
                    }
                }
            }

            /// Split at the type's declared default exchange.
            #[inline]
            pub const fn parts(
                self,
            ) -> ::core::result::Result<$crate::ExchangeParts, $crate::DomainError> {
                match Self::DEFAULT_EXCHANGE {
                    ::core::option::Option::Some(exchange) => self.parts_at(exchange),
                    ::core::option::Option::None => {
                        ::core::result::Result::Err($crate::DomainError::NoFixedOffset {
                            exchange: ::core::stringify!($name),
                        })
                    }
                }
            }
        }

        impl ::core::convert::TryFrom<i64> for $name {
            type Error = ::core::convert::Infallible;
            #[inline]
            fn try_from(raw: i64) -> ::core::result::Result<Self, Self::Error> {
                ::core::result::Result::Ok(Self::from_raw(raw))
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                ::core::write!(f, "{}({})", self.raw(), Self::UNIT.name())
            }
        }

        $crate::__domain_serde_ts!($name);
        $crate::__domain_sqlx_ts!($name);
    };
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;
    use std::sync::LazyLock;
    use regex::Regex;

    domain_int!(AnyI64, i64);
    domain_int!(NonNeg, i64, min = 0);
    domain_int!(RetryCnt, i32, max = 10);
    domain_int!(Age, i64, min = 0, max = 200);

    static ACCOUNT_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^[A-Za-z0-9_]+$").unwrap());

    domain_string!(Remark, max_chars = 4);
    domain_string!(Account, max_chars = 8, not_empty, pattern = &ACCOUNT_RE);
    domain_string!(BrokerCode, max_chars = 16);

    #[test]
    fn int_boundaries() {
        assert_eq!(Age::MIN, Some(0));
        assert_eq!(Age::MAX, Some(200));
        assert!(Age::new(0).is_ok());
        assert!(Age::new(200).is_ok());
        assert_eq!(
            Age::new(-1),
            Err(DomainError::TooSmall { ty: "Age", min: 0, value: -1 })
        );
        assert_eq!(
            Age::new(201),
            Err(DomainError::TooLarge { ty: "Age", max: 200, value: 201 })
        );
        assert_eq!(Age::new(100).unwrap().get(), 100);
    }

    #[test]
    fn int_optional_bounds() {
        assert_eq!(AnyI64::MIN, None);
        assert_eq!(AnyI64::MAX, None);
        assert!(AnyI64::new(i64::MIN).is_ok());
        assert!(AnyI64::new(i64::MAX).is_ok());

        assert!(NonNeg::new(0).is_ok());
        assert!(NonNeg::new(i64::MAX).is_ok());
        assert!(NonNeg::new(-1).is_err());

        assert!(RetryCnt::new(10).is_ok());
        assert!(RetryCnt::new(11).is_err());
        assert!(RetryCnt::new(-100).is_ok()); // upper bound only
    }

    #[test]
    fn int_try_from_and_ord() {
        let a: Age = 10i64.try_into().unwrap();
        let b: Age = 20i64.try_into().unwrap();
        assert!(a < b);
        assert_eq!(a.get(), 10);
    }

    #[test]
    fn string_cases() {
        // Length only: empty string allowed
        assert!(Remark::new("").is_ok());
        assert!(Remark::new("abcd").is_ok());
        assert!(Remark::new("abcde").is_err());

        // Character count, not byte count: two CJK chars = 6 bytes
        assert_eq!(Remark::new("\u{4f60}\u{597d}").unwrap().len_chars(), 2);
        // Five CJK chars (15 bytes) exceed the 4-character limit
        assert!(Remark::new("\u{4f60}\u{597d}\u{5440}\u{9e1f}\u{732b}").is_err());

        // not_empty + regex
        assert!(Account::new("").is_err());
        assert!(Account::new("ab_c").is_ok());
        assert!(Account::new("ab-c").is_err());
        assert!(Account::new("123456789").is_err()); // too long

        // Nullability lives in the outer Option
        let v: Option<BrokerCode> = None;
        assert!(v.is_none());
        assert!(BrokerCode::new("").is_ok()); // empty string allowed per type
        assert_eq!(BrokerCode::new("0001").unwrap().as_str(), "0001");
    }

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

    // Index form (@ k == 1 << k), mixed with an explicit value, i64 storage.
    domain_flags!(
        IdxPerms, i64 {
            READ   @ 0 => can_read;
            WRITE  @ 1 => can_write;
            DELETE = 4 => can_delete;
        }
    );

    #[test]
    fn int_state_enum() {
        assert_eq!(AccountState::ALL.len(), 3);
        let s = AccountState::from_code(1).unwrap();
        assert!(s.is_normal());
        assert!(!s.is_forbidden());
        assert_eq!(s.code(), 1);
        assert_eq!(s.name(), "Normal");

        assert_eq!(
            AccountState::from_code(99),
            Err(DomainError::UnknownCode { ty: "AccountState", value: 99 })
        );
        let via_try: AccountState = 3i32.try_into().unwrap();
        assert!(via_try.is_email_validate());
    }

    #[test]
    fn str_state_enum() {
        let b = Side::from_tag("B").unwrap();
        assert!(b.is_buy());
        assert_eq!(b.tag(), "B");
        assert_eq!(format!("{b}"), "B");
        assert!(Side::from_tag("X").is_err());
        let via_try: Side = "S".try_into().unwrap();
        assert!(via_try.is_sell());
    }

    #[test]
    fn bit_flags_business_methods() {
        // 7 = Read | Write | Delete
        let p = Permissions::from_bits(7).unwrap();
        assert!(p.can_read() && p.can_write() && p.can_delete());
        assert!(!p.can_update());
        assert!(p.contains(Permissions::READ | Permissions::WRITE));

        // 3 = Read | Write only
        let q = Permissions::from_bits(3).unwrap();
        assert!(q.can_read() && q.can_write());
        assert!(!q.can_delete());

        // Operator composition, no raw bit math at call sites
        assert_eq!((Permissions::READ | Permissions::WRITE).bits(), 3);
        assert_eq!((Permissions::MASK - Permissions::UPDATE).bits(), 7);

        let mut x = Permissions::READ;
        x |= Permissions::DELETE;
        assert!(x.can_delete());
        x -= Permissions::READ;
        assert!(!x.can_read());

        assert!(Permissions::EMPTY.is_empty());
        assert_eq!(Permissions::MASK.bits(), 15);

        // Undefined/reserved bits are rejected
        assert_eq!(
            Permissions::from_bits(16),
            Err(DomainError::UnknownBits {
                ty: "Permissions",
                value: 16,
                valid_mask: 15,
            })
        );
    }

    #[test]
    fn bit_flags_index_form() {
        assert_eq!(IdxPerms::READ.bits(), 1);
        assert_eq!(IdxPerms::WRITE.bits(), 2);
        assert_eq!(IdxPerms::DELETE.bits(), 4);
        assert_eq!(IdxPerms::MASK.bits(), 7);
        let p = IdxPerms::from_bits(3).unwrap();
        assert!(p.can_read() && p.can_write());
        assert!(!p.can_delete());
        assert!(IdxPerms::from_bits(8).is_err()); // undefined bit
    }

    domain_date!(TradeDate);
    domain_date!(YdmDate, order = ydm);
    domain_time!(TimeSec);
    domain_time!(TimeMs, frac = 3);
    domain_time!(TimeUs, frac = 6);
    domain_timestamp!(EventTime, unit = micros);
    domain_timestamp!(ShEventTime, unit = millis, exchange = sse);

    #[test]
    fn date_compact_roundtrip() {
        // 20260930 <==> 2026-09-30
        let d = TradeDate::from_compact(20260930).unwrap();
        assert_eq!((d.year(), d.month(), d.day()), (2026, 9, 30));
        assert_eq!(d.to_compact(), 20260930);
        assert_eq!(d.to_iso_string(), "2026-09-30");
        assert_eq!(format!("{d}"), "2026-09-30");
        assert_eq!(TradeDate::parse("20260930").unwrap(), d);
        assert_eq!(TradeDate::parse("2026-09-30").unwrap(), d);
        assert_eq!(TradeDate::from_ymd(2026, 9, 30).unwrap(), d);
    }

    #[test]
    fn date_separator_and_padding() {
        let d = TradeDate::from_ymd(2026, 9, 3).unwrap();
        // Slash and hyphen, padded or not, all accepted.
        assert_eq!(TradeDate::parse("2026/9/3").unwrap(), d);
        assert_eq!(TradeDate::parse("2026/09/03").unwrap(), d);
        assert_eq!(TradeDate::parse("2026-9-3").unwrap(), d);
        assert_eq!(TradeDate::parse("2026-09-03").unwrap(), d);
        // Whitespace is trimmed.
        assert_eq!(TradeDate::parse("  2026/9/3 ").unwrap(), d);
    }

    #[test]
    fn date_field_order() {
        let sep3 = TradeDate::from_ymd(2026, 9, 3).unwrap();

        // Same ambiguous source, different explicit order.
        assert_eq!(
            TradeDate::parse_fmt("2026/9/3", DateOrder::Ymd).unwrap(),
            sep3
        );
        assert_eq!(
            TradeDate::parse_fmt("2026/3/9", DateOrder::Ydm).unwrap(),
            sep3
        );
        // Declared-order parsing on a YDM type.
        let e = YdmDate::from_ymd(2026, 9, 3).unwrap();
        assert_eq!(e.to_compact(), 20260309); // YYYY DD MM on the wire
        assert_eq!(YdmDate::from_compact(20260309).unwrap(), e);
        assert_eq!(YdmDate::parse("2026/3/9").unwrap(), e);
        assert_eq!((e.year(), e.month(), e.day()), (2026, 9, 3));
        assert_eq!(e.to_iso_string(), "2026-09-03");
        // Reading a YMD-produced integer with the wrong order yields the
        // swapped components (here an invalid month 30) -> rejected.
        assert!(YdmDate::from_compact(20260930).is_err());
        // Cross-order compact conversion canonicalizes into the type order.
        assert_eq!(YdmDate::from_compact_fmt(20260903, DateOrder::Ymd).unwrap(), e);
        // Arithmetic keeps the declared wire order.
        assert_eq!(e.add_days(1).to_compact(), 20260409);
    }

    #[test]
    fn date_rejects_bad_calendar() {
        // 2026-02-30 does not exist
        assert!(matches!(
            TradeDate::from_compact(20260230),
            Err(DomainError::BadDate { value: 20260230, .. })
        ));
        // month 13
        assert!(TradeDate::from_compact(20261301).is_err());
        // malformed strings
        assert!(TradeDate::parse("2026/9/3/1").is_err());    // too many fields
        assert!(TradeDate::parse("2026-9/3").is_err());       // mixed separators
        assert!(TradeDate::parse("abcd-09-30").is_err());
        assert!(TradeDate::parse("26-09-30").is_err());       // 2-digit year
        assert!(TradeDate::parse("2026-00-30").is_err());
        assert!(TradeDate::parse("2026-9-31").is_err());      // Sep has 30 days
    }

    #[test]
    fn date_leap_year_rules() {
        // 2024 is a leap year: Feb 29 exists
        assert!(TradeDate::from_compact(20240229).is_ok());
        // 2026 is not: Feb 29 rejected
        assert!(TradeDate::from_compact(20260229).is_err());
        // 2000 is a leap year (divisible by 400)
        assert!(TradeDate::from_compact(20000229).is_ok());
        // 1900 is not (divisible by 100 but not 400)
        assert!(TradeDate::from_compact(19000229).is_err());
    }

    #[test]
    fn date_weekday_and_arithmetic() {
        let d = TradeDate::from_compact(20260930).unwrap();
        assert_eq!(d.weekday(), 3); // Wednesday (0=Sun)
        assert_eq!(d.weekday_monday0(), 2); // Wednesday (0=Mon)
        assert!(!d.is_weekend());

        // Cross-month T+1
        assert_eq!(d.add_days(1).to_compact(), 20261001);
        // Cross-year
        assert_eq!(
            TradeDate::from_compact(20261231).unwrap().add_days(1).to_compact(),
            20270101
        );
        // Feb 28 -> +1 on a leap year lands on Feb 29
        assert_eq!(
            TradeDate::from_compact(20240228).unwrap().add_days(1).to_compact(),
            20240229
        );
        let d2 = TradeDate::from_compact(20261003).unwrap();
        assert_eq!(d2.diff_days(d), 3);
        assert!(d2.is_weekend()); // 2026-10-03 is a Saturday
    }

    #[test]
    fn time_seconds_roundtrip() {
        // 120403 <==> 12:04:03
        let t = TimeSec::from_packed(120403).unwrap();
        assert_eq!((t.hour(), t.minute(), t.second()), (12, 4, 3));
        assert_eq!(t.frac(), 0);
        assert_eq!(t.to_packed(), 120403);
        assert_eq!(t.to_hms_string(), "12:04:03");
        assert_eq!(TimeSec::parse("12:04:03").unwrap(), t);
        assert_eq!(TimeSec::parse("120403").unwrap(), t);
    }

    #[test]
    fn time_millis_roundtrip() {
        // 120403333 <==> 12:04:03.333
        let t = TimeMs::from_packed(120403333).unwrap();
        assert_eq!((t.hour(), t.minute(), t.second()), (12, 4, 3));
        assert_eq!(t.frac(), 333);
        assert_eq!(t.to_hms_string(), "12:04:03.333");
        assert_eq!(TimeMs::parse("12:04:03.333").unwrap(), t);
        assert_eq!(TimeMs::parse("120403.333").unwrap(), t);

        // ".3" is padded to 300 ms
        assert_eq!(TimeMs::parse("12:04:03.3").unwrap().frac(), 300);
        // Extra digits may only be zeros
        assert!(TimeMs::parse("12:04:03.3330").is_ok());
        assert!(TimeMs::parse("12:04:03.334").unwrap().frac() == 334);
        // Nonzero beyond precision is rejected (no silent truncation)
        assert!(TimeMs::parse("12:04:03.3331").is_err());
    }

    #[test]
    fn time_validation() {
        assert!(TimeSec::from_packed(126003).is_err()); // minute 60
        assert!(TimeSec::from_packed(240000).is_err()); // hour 24
        assert!(TimeSec::from_packed(-1).is_err());
        assert!(TimeMs::from_packed(120403999).is_ok());
        assert!(TimeMs::from_packed(120403000).is_ok());
        assert!(TimeMs::from_packed(120460000).is_err()); // second 60 only at 23:59
        // leap second slot 23:59:60 is allowed
        assert!(TimeMs::from_packed(235960000).is_ok());
        assert!(TimeSec::parse("12:4:03").is_err());
        assert!(TimeSec::parse("12:04:03.5").is_err()); // scale 0 rejects nonzero fraction

        // microsecond scale lives in i64/int8
        let u = TimeUs::from_packed(120403333000).unwrap();
        assert_eq!(u.frac(), 333000);
        assert_eq!(u.to_hms_string(), "12:04:03.333000");
    }

    #[test]
    fn timestamp_unit_conversion() {
        // Milliseconds on the wire, microseconds canonical.
        let t = ShEventTime::from_raw(123);
        assert_eq!(t.raw(), 123);
        assert_eq!(t.epoch_micros(), 123_000);

        // Microsecond type is identity.
        assert_eq!(EventTime::from_epoch_micros(7).raw(), 7);

        // Nanoseconds truncate floor toward negative infinity.
        domain_timestamp!(NanoTime, unit = nanos);
        assert_eq!(NanoTime::from_raw(1_500).epoch_micros(), 1);
        assert_eq!(NanoTime::from_epoch_micros(2).raw(), 2_000);
        assert_eq!(NanoTime::from_raw(-1_500).epoch_micros(), -2);
    }

    #[test]
    fn timestamp_exchange_day_boundary() {
        let epoch = EventTime::from_epoch_micros(0);

        // 1970-01-01 00:00:00 UTC
        let utc = epoch.parts_at(Exchange::Utc).unwrap();
        assert_eq!((utc.year, utc.month, utc.day), (1970, 1, 1));
        assert_eq!((utc.hour, utc.minute, utc.second, utc.microsecond), (0, 0, 0, 0));
        assert_eq!(utc.hhmmss(), 0);

        // Shanghai / Hong Kong = UTC+8; Tokyo = UTC+9.
        let sh = epoch.parts_at(Exchange::Sse).unwrap();
        assert_eq!((sh.year, sh.month, sh.day, sh.hour), (1970, 1, 1, 8));
        assert_eq!(epoch.parts_at(Exchange::Hkex).unwrap(), sh);
        let tk = epoch.parts_at(Exchange::Tse).unwrap();
        assert_eq!((tk.year, tk.month, tk.day, tk.hour), (1970, 1, 1, 9));

        // Declared default exchange works without an argument.
        assert_eq!(ShEventTime::from_raw(0).parts().unwrap(), sh);

        // A UTC instant just before midnight crosses the date in Shanghai.
        let before_midnight = EventTime::from_epoch_micros(16 * 3600 * 1_000_000 - 1);
        let p = before_midnight.parts_at(Exchange::Sse).unwrap();
        assert_eq!((p.year, p.month, p.day), (1970, 1, 1));
        assert_eq!((p.hour, p.minute, p.second, p.microsecond), (23, 59, 59, 999_999));
    }

    #[test]
    fn timestamp_negative_epoch_uses_euclidean_day() {
        // One second before epoch: 1969-12-31 23:59:59 UTC, 1970-01-01 07:59:59 SSE.
        let t = EventTime::from_epoch_micros(-1_000_000);
        let utc = t.parts_at(Exchange::Utc).unwrap();
        assert_eq!((utc.year, utc.month, utc.day, utc.hour, utc.minute, utc.second),
                   (1969, 12, 31, 23, 59, 59));
        let sh = t.parts_at(Exchange::Sse).unwrap();
        assert_eq!((sh.year, sh.month, sh.day, sh.hour, sh.minute, sh.second),
                   (1970, 1, 1, 7, 59, 59));
    }

    #[test]
    fn timestamp_dst_venues_require_explicit_offset() {
        let t = EventTime::from_epoch_micros(0);
        for ex in [Exchange::Lse, Exchange::Nyse, Exchange::Nasdaq, Exchange::Cme, Exchange::Ice] {
            assert!(ex.observes_dst());
            assert_eq!(
                t.parts_at(ex),
                Err(DomainError::NoFixedOffset { exchange: ex.code() })
            );
        }
        // Explicit offset from a session calendar is the supported escape hatch:
        // London summer time UTC+1 -> 01:00 on 1970-01-01 for demonstration.
        let p = t.parts_at_offset(60);
        assert_eq!((p.day, p.hour), (1, 1));

        // No default exchange declared -> parts() tells the caller to choose.
        assert!(EventTime::from_epoch_micros(0).parts().is_err());
    }

    #[test]
    fn exchange_codes_and_offsets() {
        assert_eq!(Exchange::Sse.code(), "SSE");
        assert_eq!(Exchange::Sse.fixed_offset_minutes(), Some(480));
        assert_eq!(Exchange::Szse.fixed_offset_minutes(), Some(480));
        assert_eq!(Exchange::Hkex.fixed_offset_minutes(), Some(480));
        assert_eq!(Exchange::Tse.fixed_offset_minutes(), Some(540));
        assert_eq!(Exchange::Krx.fixed_offset_minutes(), Some(540));
        assert_eq!(Exchange::Sgx.fixed_offset_minutes(), Some(480));
        assert_eq!(Exchange::Utc.fixed_offset_minutes(), Some(0));
    }
}
