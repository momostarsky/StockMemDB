# 交易所时间常量与 UTC 时间转换

## 1. 设计原则

1. **内部统一存储 UTC epoch 微秒**（`i64`，单位：微秒）。
2. `TimeUnit` 只表示消息线路上的时间精度：
   - `millis`：毫秒
   - `micros`：微秒（平台内部标准）
   - `nanos`：纳秒（PTP / 硬件时间戳场景）
3. `Exchange` 表示交易所时钟，用于将 UTC 时间转换为交易所本地日期与时间。
4. **时间精度与交易所是两个正交概念**，不能合并到同一个 `unit` 参数中。
5. 存在夏令时的交易所没有固定 UTC 偏移，必须通过交易日历或场次表按日期提供偏移。
6. 夜盘交易日归属不由 `Exchange` 决定，需要独立的 `SessionCalendar`。

## 2. Rust 类型定义

```rust
pub enum TimeUnit {
    Millis,
    Micros,
    Nanos,
}

pub enum Exchange {
    Utc,
    Sse,
    Szse,
    Hkex,
    Tse,
    Krx,
    Sgx,
    Lse,
    Nyse,
    Nasdaq,
    Cme,
    Ice,
}
```

## 3. 交易所常量表

| Rust 常量 | 机器代码 | 交易所/市场 | 时区 | 固定偏移（分钟） | 固定偏移（小时） | 是否夏令时 | 说明 |
|---|---|---|---|---:|---:|---|---|
| `Exchange::Utc` | `UTC` | UTC 参考时钟 | UTC | `0` | `+0` | 否 | 不归属具体交易所 |
| `Exchange::Sse` | `SSE` | 上海证券交易所 / 上海期货市场 | CST / Asia/Shanghai | `480` | `+8` | 否 | 中国标准时间 |
| `Exchange::Szse` | `SZSE` | 深圳证券交易所 | CST / Asia/Shanghai | `480` | `+8` | 否 | 中国标准时间 |
| `Exchange::Hkex` | `HKEX` | 香港交易及结算所 | HKT / Asia/Hong_Kong | `480` | `+8` | 否 | 香港自 1979 年后无夏令时 |
| `Exchange::Tse` | `TSE` | 东京证券交易所 / JPX | JST / Asia/Tokyo | `540` | `+9` | 否 | 日本标准时间 |
| `Exchange::Krx` | `KRX` | 韩国交易所 | KST / Asia/Seoul | `540` | `+9` | 否 | 韩国标准时间 |
| `Exchange::Sgx` | `SGX` | 新加坡交易所 | SGT / Asia/Singapore | `480` | `+8` | 否 | 新加坡 1981 年后无夏令时 |
| `Exchange::Lse` | `LSE` | 伦敦证券交易所 | GMT / BST | 无固定值 | 无固定值 | 是 | 冬季 GMT+0，夏季 BST+1 |
| `Exchange::Nyse` | `NYSE` | 纽约证券交易所 | EST / EDT | 无固定值 | 无固定值 | 是 | 冬季 EST-5，夏季 EDT-4 |
| `Exchange::Nasdaq` | `NASDAQ` | 纳斯达克 | EST / EDT | 无固定值 | 无固定值 | 是 | 冬季 EST-5，夏季 EDT-4 |
| `Exchange::Cme` | `CME` | CME Group | CST / CDT 等 | 无固定值 | 无固定值 | 是 | 多数美国期货受夏令时影响 |
| `Exchange::Ice` | `ICE` | Intercontinental Exchange | EST / EDT、GMT / BST 等 | 无固定值 | 无固定值 | 是 | 具体产品按交易场所与场次确定 |

> 偏移统一采用“相对 UTC 向东的分钟数”（minutes east of UTC）。例如上海为 `8 × 60 = 480`。

## 4. 固定偏移交易所

以下交易所在 `fixed_offset_minutes()` 中返回 `Some(offset)`：

| 交易所 | `fixed_offset_minutes()` |
|---|---:|
| `Exchange::Utc` | `Some(0)` |
| `Exchange::Sse` | `Some(480)` |
| `Exchange::Szse` | `Some(480)` |
| `Exchange::Hkex` | `Some(480)` |
| `Exchange::Tse` | `Some(540)` |
| `Exchange::Krx` | `Some(540)` |
| `Exchange::Sgx` | `Some(480)` |

这些市场可以直接使用：

```rust
let parts = event_time.parts_at(Exchange::Sse)?;
```

## 5. 夏令时交易所

以下交易所返回 `None`：

- `Exchange::Lse`
- `Exchange::Nyse`
- `Exchange::Nasdaq`
- `Exchange::Cme`
- `Exchange::Ice`

直接调用：

```rust
let parts = event_time.parts_at(Exchange::Lse)?;
```

会返回：

```rust
DomainError::NoFixedOffset { exchange: "LSE" }
```

正确方式是由交易日历或场次配置提供当日偏移：

```rust
let offset_minutes = session_calendar.offset_minutes(Exchange::Lse, trade_date);
let parts = event_time.parts_at_offset(offset_minutes);
```

这样可以避免把伦敦或纽约的冬令时/夏令时偏移错误地硬编码进类型系统。

## 6. 时间戳宏定义

```rust
domain_timestamp!(pub EventTime, unit = micros);
domain_timestamp!(pub ShEventTime, unit = millis, exchange = sse);
```

宏参数：

| 参数 | 必填 | 取值 | 说明 |
|---|---|---|---|
| `unit` | 是 | `millis` / `micros` / `nanos` | 消息线路时间单位 |
| `exchange` | 否 | `utc` / `sse` / `szse` / `hkex` / `tse` / `krx` / `sgx` / `lse` / `nyse` / `nasdaq` / `cme` / `ice` | 类型默认交易所 |

非法值会在编译期失败：

```rust
domain_timestamp!(BadUnit, unit = seconds);
// compile_error!: domain_timestamp! unit must be millis, micros or nanos

domain_timestamp!(BadExchange, unit = micros, exchange = ny);
// compile_error!: unknown exchange
```

## 7. 内部值与线路值

```rust
let t = ShEventTime::from_raw(123);

assert_eq!(t.raw(), 123);          // 线路值：毫秒
assert_eq!(t.epoch_micros(), 123_000); // 内部规范值：微秒
```

| 线路单位 | 线路值示例 | 内部 epoch 微秒 |
|---|---:|---:|
| `millis` | `123` | `123000` |
| `micros` | `123000` | `123000` |
| `nanos` | `123000000` | `123000` |

纳秒转微秒使用向负无穷取整（floor），保证负 epoch 的日期切分一致。

## 8. UTC 转交易所日期时间

### 8.1 固定偏移交易所

```rust
let t = EventTime::from_epoch_micros(0); // 1970-01-01 00:00:00 UTC

let sh = t.parts_at(Exchange::Sse)?;
assert_eq!(sh.year, 1970);
assert_eq!(sh.month, 1);
assert_eq!(sh.day, 1);
assert_eq!(sh.hour, 8);
```

### 8.2 类型默认交易所

```rust
domain_timestamp!(ShEventTime, unit = millis, exchange = sse);

let parts = ShEventTime::from_raw(0).parts()?;
assert_eq!(parts.hour, 8);
```

### 8.3 显式偏移

```rust
// 假设 session calendar 判定伦敦当日为夏令时 BST，UTC+1
let parts = event_time.parts_at_offset(60);
```

## 9. 转换为 TradeDate / TradeTime

```rust
let parts = event_time.parts_at(Exchange::Sse)?;

let trade_date = TradeDate::from_ymd(parts.year, parts.month, parts.day)?;

let trade_time = TradeTimeUs::from_hms_frac(
    parts.hour,
    parts.minute,
    parts.second,
    parts.microsecond,
)?;
```

`ExchangeParts` 字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| `year` | `i32` | 交易所本地年 |
| `month` | `u8` | 月，1..=12 |
| `day` | `u8` | 日，1..=31 |
| `hour` | `u8` | 小时，0..=23 |
| `minute` | `u8` | 分钟，0..=59 |
| `second` | `u8` | 秒，0..=60；60 仅用于闰秒场景 |
| `microsecond` | `i64` | 秒内微秒，0..=999999 |

## 10. PG 存储建议

| 场景 | 推荐列类型 | 应用类型 | 说明 |
|---|---|---|---|
| 常规事件表、审计表、归档表 | `timestamptz` | 边界映射到 epoch 微秒 | PG 内部为 8 字节微秒，时间函数和分区能力最好 |
| 高频只追加事件表 | `int8` / `bigint` | `EventTime.epoch_micros()` | 与线路真相一致，编码成本最低 |
| 交易日冗余列 | `int4` | `TradeDate.to_compact()` | 由应用层按交易所偏移计算后写入 |
| 日内交易时间字段 | `int8` / `int4` | `TradeTimeUs` / `TradeTimeMs` | 协议字段与展示字段，不用于跨日排序 |

推荐数据库统一配置：

```sql
SET timezone = 'UTC';
SET log_timezone = 'UTC';
```

## 11. 夜盘与交易日归属

`ExchangeParts` 返回的是交易所本地日历日期，不一定是业务交易日。

例如国内期货夜盘：

- 本地日历日：周一晚上
- 业务交易日：可能归属为周二

该规则需要独立组件：

```rust
trait SessionCalendar {
    fn session_of(
        &self,
        exchange: Exchange,
        parts: ExchangeParts,
        instrument_id: u32,
    ) -> Result<TradeDate, DomainError>;
}
```

处理内容包括：

- 夜盘开盘归属
- 午休与连续/不连续交易时段
- 节假日
- 半日市
- 临时休市
- 夏令时切换日
- 交易所特殊交易日历

## 12. 关键纪律

1. 排序与去重使用 `(epoch_micros, seq)`。
2. 不使用本地机器时区，不依赖容器的 `TZ` 环境变量。
3. 固定偏移交易所可直接调用 `parts_at()`。
4. 夏令时交易所必须通过 `parts_at_offset()` 传入交易日历解析后的偏移。
5. `TradeDate` / `TradeTime` 是业务展示与线路字段，UTC epoch 微秒才是唯一时间真相。
