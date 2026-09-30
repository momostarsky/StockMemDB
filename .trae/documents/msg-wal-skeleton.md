# msg-wal M0 骨架实现计划

## Context

brsk-msgx（Rust 金融高性能二进制消息转发平台）的 M0 收官项是 **msg-wal 骨架**。Roadmap §3.1/§4/§9 M1 明确要求：

- "WAL 段 = 线路字节直存（线路格式即存储格式，恢复时零解析扫描）"——段文件直接拼接 msg-proto 线路帧（48B 头 + body），**不包 WAL record header**。
- mmap WAL（memmap2 + 自研 segment）+ 顺序追加、group commit、CRC32C、段回收。
- 写入端：producer → write WAL → fdatasync 成功 → ACK；启动崩溃恢复扫描；fsync P99 SLI watchdog；disk-full 熔断。
- Raft log 与 WAL 合一（M4 openraft log storage 直接落 mmap 段文件）——offset 模型需兼容 openraft LogId.index。

M0 骨架范围：open/append/group_commit/sync/recover + 基本指标；Windows 文件 IO 先行，Linux mmap 后续（M1）。**不实现** memmap2、段回收、Raft 集成、ACK 语义、消费 offset、去重窗口——但 API 形态要为这些留口子。

已落地依赖：`msg-proto` 提供 `encode_frame`/`decode_frame`/`peek_total_len`/`DecodedFrame`/`FrameHeader`/`HEADER_SIZE=48`/`MAX_BODY_LEN=16MiB`/`ProtoError`，WAL 复用而不重实现 CRC。

## 已签字决策

| # | 决策 | 理由 |
|---|---|---|
| 段大小 | 默认 64 MiB，`WalConfig.max_segment_bytes` 可配 | 匹配 §7.2 group commit 吞吐估算（256 帧/fsync × ~256B ≈ 64KiB/fsync） |
| Offset 模型 | `WalOffset(pub u64)` flat 逻辑条目索引，0 起单调递增，跨段不重置 | 1:1 对接 M4 openraft `LogId.index` 与 M2 消费 offset；无段大小上限（mmap 段在 Linux 上可超 4GiB，packed `(seg<<32\|byte)` 模型被排除） |
| 段文件名 | `segment-{index:08}.log`（8 位零填充，字典序=索引序） | 恢复按字典序枚举；8 位支持 10^8 段 |
| 不包 record header | 段文件 = msg-proto 帧字节拼接 | §3.1 第 3 点逐字要求 |
| 段滚动策略 | 写入前若 `byte_pos + frame_len > max_segment_bytes` 则先滚动；帧绝不跨段 | 恢复 + M1 mmap page-aligned per frame |
| 帧过大 | `frame_len > max_segment_bytes` 返回 `WalError::FrameTooLarge`；config 校验 `max_segment_bytes > HEADER_SIZE + MAX_BODY_LEN` | 默认 64MiB 远大于 48+16MiB，合法帧必能入空段 |
| Group commit API | **BatchBuilder 游标**：`wal.batch().append(h,b)?.append(h,b)?.commit()?` | 用户指定；M1 与 monoio ring 排空 + 阈值 flush 更贴合；M0 编码立即累计、commit 时一次 write_all + 一次 sync_data |
| open 语义 | `Wal::open` 自动跑 `recover_dir`，返回 `(Wal, RecoveryReport)`；另提供 `Wal::recover(dir)` 只读巡检 | 安全默认，永不因忘记 recover 而丢数据 |
| 并发 | `Wal: Send` 非 `Sync`；`append`/`batch`/`sync` 取 `&mut self`——单写者在类型层强制 | 与 monoio 数据面单线程对齐；`WalMetrics` 经 `Arc` 克隆出供监控线程 |
| `StorageBackend` trait | M0 即引入（不延后到 M1） | 用户偏好模块化 + trait 接口；80 行代码换 M1 mmap swap 一行替换 |
| 错误类型 | `#[derive(Debug)]` enum + 手动 `Display` + `impl Error`，**不用 thiserror** | 项目约定（msg-proto/msg-transport/msg-account/msg-domain 一致） |
| fsync 调用 | `File::sync_data()` 一处可移植路径 | Linux=fdatasync，Windows=FlushFileBuffers（== sync_all），无需 cfg |
| 崩溃恢复 | clean truncation（`peek_total_len` 返回 `Ok(None)`）→ 截尾继续下一段；corruption（`Err(ProtoError::*)` 或 `decode_frame` `Err(CrcMismatch)`）→ 截尾、quarantine、**STOP** | Raft log contiguity 要求：gap 即致命，不能跳过损坏继续 |

## A. Crate 布局

```
crates/msg-wal/
  Cargo.toml
  src/
    lib.rs        # crate doc + WalError (inline) + WalResult + pub use
    config.rs     # WalConfig + Default + validate()
    offset.rs     # WalOffset newtype + segment-name helpers (in segment.rs)
    segment.rs    # SegmentName + Segment (in-mem state)
    backend.rs    # StorageBackend trait + SegmentFile + FileIoBackend
    metrics.rs    # WalMetrics (AtomicU64) + MetricsSnapshot
    recovery.rs   # RecoveryReport + Corruption + CorruptionKind + recover_dir()
    batch.rs      # BatchBuilder (group commit cursor)
    wal.rs        # Wal struct: open/recover/append/batch/sync/metrics
  tests/
    wal.rs        # 7 个集成测试
```

## B. 公共 API（签名）

```rust
//! # msg-wal
//! Write-ahead log for brsk-msgx (Roadmap §4, §9 M1). The WAL is the single
//! source of truth: a frame is ACKed only after fsync here; Raft (M4)
//! replicates this log; consumers (M2) ACK by offset.
//!
//! ## Storage contract (Roadmap §3.1 — "线路字节直存")
//! A segment file is a concatenation of msg-proto wire frames (48-byte header
//! ++ body) with NO WAL record header. Append == append encoded frame bytes.
//! Recovery reuses `msg_proto::peek_total_len` + `decode_frame`; the WAL never
//! reimplements CRC.
//!
//! ## Offset model
//! `WalOffset(u64)` is a logical entry index: 0 for the first appended frame,
//! +1 per frame, monotonic across segment rolls. It is the consumer offset
//! (M2) and the openraft log index (M4).
//!
//! ## Atomicity (group commit via BatchBuilder)
//! `wal.batch().append(h,b)?.append(h,b)?.commit()?` accumulates encoded
//! frames in memory; `commit()` writes them as one buffer + one sync_data.
//! Crash before sync_data returns => none durable; after => all durable.
//! Any `encode_frame` failure poisons the builder; `commit()` returns the
//! error and nothing is written.
//!
//! ## Backend swappability (M1)
//! IO is isolated behind `StorageBackend`. M0 ships `FileIoBackend`
//! (std::fs::File + sync_data); M1 swaps `MmapBackend` (memmap2) behind the
//! same trait.
//!
//! ## Concurrency
//! `Wal: Send` (not `Sync`); append/batch/sync take `&mut self` — single
//! writer at the type level. `WalMetrics` is `Send + Sync` via `Arc`.
//!
//! ```no_run
//! use msg_proto::FrameHeader;
//! use msg_wal::{Wal, WalConfig};
//! let dir = std::env::temp_dir().join("wal-demo");
//! let (mut wal, report) = Wal::open(&dir, WalConfig::default()).unwrap();
//! assert_eq!(report.frames_recovered, 0);
//! wal.append(FrameHeader::publish(42, 1, 7, 0, 4), &[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
//! let offs = wal.batch()
//!     .append(FrameHeader::publish(42, 1, 7, 1, 2), &b"hi"[..]).unwrap()
//!     .append(FrameHeader::publish(42, 1, 7, 2, 2), &b"yo"[..]).unwrap()
//!     .commit().unwrap();
//! wal.sync().unwrap();
//! drop(wal);
//! let (wal2, report) = Wal::open(&dir, WalConfig::default()).unwrap();
//! assert_eq!(report.frames_recovered, 3);
//! ```

mod backend; mod batch; mod config; mod metrics; mod offset; mod recovery; mod segment; mod wal;

pub use backend::{FileIoBackend, SegmentFile, StorageBackend};
pub use batch::BatchBuilder;
pub use config::WalConfig;
pub use metrics::{MetricsSnapshot, WalMetrics};
pub use offset::WalOffset;
pub use recovery::{Corruption, CorruptionKind, RecoveryReport};
pub use segment::SegmentName;
pub use wal::Wal;

pub type WalResult<T> = Result<T, WalError>;

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    DiskFull,                                      // StorageFull / WriteZero 分类
    FrameTooLarge { frame_len: usize, max_segment: usize },
    Encode(msg_proto::ProtoError),                // append/encode 失败
    Decode(msg_proto::ProtoError),                // 恢复时帧损坏
    BadConfig(&'static str),
    InconsistentState(String),                     // 段索引 gap
    Poisoned,                                      // 上次 commit 失败后 Wal 被毒化
}

impl fmt::Display for WalError { /* ... */ }
impl std::error::Error for WalError {}
impl From<io::Error> for WalError {
    fn from(e) -> Self {
        match e.kind() {
            io::ErrorKind::StorageFull | io::ErrorKind::WriteZero => WalError::DiskFull,
            _ => WalError::Io(e),
        }
    }
}
```

```rust
// config.rs
pub struct WalConfig {
    pub max_segment_bytes: usize,     // default 64 * 1024 * 1024
    pub fsync_p99_target_us: u64,      // default 100, M2 watchdog 用
}
impl Default for WalConfig { /* 64MiB, 100us */ }
impl WalConfig { pub fn validate(&self) -> WalResult<()> { /* max > HEADER+MAX_BODY; target>0 */ } }

// offset.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WalOffset(pub u64);
impl WalOffset {
    pub const fn new(v: u64) -> Self;
    pub const fn get(self) -> u64;
    pub const fn next(self) -> Self;     // Self(self.0 + 1)
}

// segment.rs
pub struct SegmentName { pub index: u32 }
impl SegmentName {
    pub fn new(index: u32) -> Self;
    pub fn from_path(p: &Path) -> Option<Self>;     // 严格解析 "segment-NNNNNNNN.log"
    pub fn to_filename(&self) -> String;
    pub fn to_path(&self, dir: &Path) -> PathBuf;
}
pub(crate) struct Segment {
    pub index: u32,
    pub base_offset: WalOffset,
    pub byte_pos: u64,            // == file size
    pub path: PathBuf,
    pub file: SegmentFile,
}

// backend.rs
pub struct SegmentFile(pub(crate) File);
pub trait StorageBackend: Send + Sync {
    fn open_segment(&self, path: &Path) -> Result<SegmentFile, WalError>;
    fn append(&self, file: &mut SegmentFile, bytes: &[u8]) -> Result<(), WalError>;
    fn sync(&self, file: &SegmentFile) -> Result<(), WalError>;   // sync_data
    fn size(&self, file: &SegmentFile) -> Result<u64, WalError>;
    fn read_all(&self, path: &Path) -> Result<Vec<u8>, WalError>;
    fn truncate(&self, path: &Path, len: u64) -> Result<(), WalError>;
}
#[derive(Debug, Default)]
pub struct FileIoBackend;
impl StorageBackend for FileIoBackend { /* OpenOptions+append+read; write_all; sync_data; metadata.len; fs::read; set_len */ }

// metrics.rs
pub struct WalMetrics {
    pub appends: AtomicU64, pub group_commits: AtomicU64, pub frames_written: AtomicU64,
    pub fsyncs: AtomicU64, pub fsync_us_p99: AtomicU64,    // M0 last-sample; M1 rolling P99
    pub bytes_written: AtomicU64, pub recovery_truncated_bytes: AtomicU64, pub recovery_corruptions: AtomicU64,
}
pub struct MetricsSnapshot { /* u64 镜像 */ }
impl WalMetrics { pub fn new() -> Self; pub fn snapshot(&self) -> MetricsSnapshot; }

// recovery.rs
pub struct RecoveryReport {
    pub frames_recovered: u64, pub segments_scanned: u32,
    pub last_offset: Option<WalOffset>,    // 下一个待分配索引（None=空日志）
    pub truncated_bytes: u64, pub corruption: Option<Corruption>,
}
pub struct Corruption { pub segment_index: u32, pub byte_offset: u64, pub kind: CorruptionKind }
pub enum CorruptionKind {
    BadMagic(u32), BadVersion { supported: u8, got: u8 }, NonZeroReserved,
    BodyTooLarge { len: u32, max: u32 }, CrcMismatch { expected: u32, actual: u32 },
}
impl From<msg_proto::ProtoError> for CorruptionKind { /* 7 变体映射 */ }

// batch.rs
pub struct BatchBuilder<'a> {
    wal: &'a mut Wal,
    encoded: Vec<u8>,            // 已编码帧拼接
    frame_lens: Vec<usize>,
    offsets: Vec<WalOffset>,    // prospective offsets（append 时分配）
    poisoned: Option<WalError>, // 首次 encode/size 失败；后续操作复用此错
}
impl<'a> BatchBuilder<'a> {
    pub fn append(&mut self, header: msg_proto::FrameHeader, body: &[u8]) -> WalResult<&mut Self>;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn byte_len(&self) -> usize;       // 已编码字节数（阈值 flush 用）
    pub fn commit(self) -> WalResult<Vec<WalOffset>>;
    // Drop 未 commit => 丢弃已编码字节，不写不 sync（abort 语义）
}

// wal.rs
pub struct Wal {
    dir: PathBuf, config: WalConfig,
    backend: Box<dyn StorageBackend>,
    active: Segment,
    next_offset: WalOffset,
    next_segment_index: u32,
    metrics: Arc<WalMetrics>,
    poisoned: bool,             // 上次 commit/write 失败 → 拒绝后续 append
}
impl Wal {
    pub fn open(dir: &Path, config: WalConfig) -> WalResult<(Wal, RecoveryReport)>;
    pub fn recover(dir: &Path) -> WalResult<RecoveryReport>;  // 只读巡检
    pub fn append(&mut self, header: msg_proto::FrameHeader, body: &[u8]) -> WalResult<WalOffset>;
    pub fn batch(&mut self) -> BatchBuilder<'_>;
    pub fn sync(&mut self) -> WalResult<()>;
    pub fn metrics(&self) -> Arc<WalMetrics>;
    pub fn config(&self) -> &WalConfig;
    pub fn next_offset(&self) -> WalOffset;
}
```

## C. 段文件格式

字节布局：msg-proto 线路帧拼接，无 WAL 头/尾/length prefix。

```
[ 48B header0 ++ body0 ][ 48B header1 ++ body1 ] ... [ 48B headerN ++ bodyN ]
```

帧头自带 `body_len` 与 `crc32c`，`peek_total_len` 定界 + `decode_frame` 校验。

段文件名：`segment-{index:08}.log`（如 `segment-00000000.log`、`segment-00000001.log`）。8 位零填充保证字典序=索引序，恢复按字典序枚举即可。`index` 从 0 起，每次滚动 +1；**不**是 base entry offset（base 由扫描推导）。

## D. Append / BatchBuilder / sync 流程

### `append(header, body)`（单帧便利方法，内部走 batch）

```
1. 若 wal.poisoned: return Err(WalError::Poisoned)
2. wire = msg_proto::encode_frame(header, body)?;  // CRC 单一来源
3. 若 wire.len() > max_segment_bytes: return Err(FrameTooLarge)
4. 若 active.byte_pos + wire.len() > max_segment_bytes: roll_segment()?
5. offset = next_offset
6. backend.append(&active.file, &wire)?
7. active.byte_pos += wire.len(); next_offset = offset.next()
8. metrics 累加
9. Ok(offset)
```

`append` **不** fsync；调用方调 `sync()` 或用 `batch().commit()`。

### `BatchBuilder::append`

```
1. 若 poisoned.is_some(): return Err(复用错)
2. wire = encode_frame(header, body).map_err(|e| { poisoned = Some(Encode(e)); return Err })?
3. 若 wire.len() > max_segment_bytes: poisoned = Some(FrameTooLarge); return Err
4. offset = wal.next_offset + offsets.len() as u64   // prospective
5. encoded.extend_from_slice(&wire); frame_lens.push(wire.len()); offsets.push(offset)
6. Ok(self)
```

### `BatchBuilder::commit(self)`

```
1. 若 poisoned.is_some(): return Err(取出错)        // 不写
2. 若 offsets.is_empty(): return Ok(vec![])
3. cursor = 0; touched: Vec<u32> = vec![]
4. for flen in frame_lens:
     若 active.byte_pos + flen > max_segment_bytes:
        backend.sync(&active.file)?; metrics.fsyncs++; roll_segment_unsynced()?
     若 touched.last() != Some(&active.index): touched.push(active.index)
     backend.append(&active.file, &encoded[cursor..cursor+flen])?
     active.byte_pos += flen as u64; cursor += flen
5. backend.sync(&active.file)?; metrics.fsyncs++; fsync_us 计时存 p99
6. wal.next_offset += offsets.len() as u64
7. metrics.group_commits++; frames_written += offsets.len(); bytes_written += encoded.len()
8. Ok(offsets)
```

**原子性**：
- 步 2 encode 失败 → 步 4 不执行，无字节落盘。
- 步 4 写入 + 步 5 sync 之间崩溃 → OS page cache 丢，无一帧 durable。
- 步 5 sync 完成后崩溃 → 全部 durable。
- 步 4 中段滚动：旧段在滚动时已 sync（其包含 prior appends）；末段在步 5 sync。
- 步 4 write_all 中途失败 → `Wal.poisoned = true`，后续 append 返回 `WalError::Poisoned`；用户需 `Wal::open` 重新恢复（恢复扫描会截掉 partial 尾巴）。

### `sync(&mut self)`

```
1. 若 poisoned: return Err(Poisoned)
2. t0 = Instant::now()
3. backend.sync(&active.file)?
4. metrics.fsyncs++; metrics.fsync_us_p99 = t0.elapsed().as_micros()
5. Ok(())
```

### `roll_segment`（私有）

```
roll_segment():
  backend.sync(&active.file)?; metrics.fsyncs++
  roll_segment_unsynced()
roll_segment_unsynced():
  idx = next_segment_index; next_segment_index += 1
  path = SegmentName::new(idx).to_path(&dir)
  file = backend.open_segment(&path)?
  active = Segment { index: idx, base_offset: next_offset, byte_pos: 0, path, file }
```

## E. 崩溃恢复流程

```rust
pub(crate) fn recover_dir(backend: &dyn StorageBackend, dir: &Path) -> Result<RecoveryReport, WalError> {
    // 1. 字典序枚举段文件
    let mut names: Vec<SegmentName> = read_dir(dir)?.filter_map(|e| SegmentName::from_path(&e.path())).collect();
    names.sort_by_key(|n| n.index);

    // 2. 段索引 gap 检测
    for (i, n) in names.iter().enumerate() {
        if n.index as usize != i { return Err(InconsistentState(format!("segment index gap: expected {i}, got {}", n.index))); }
    }

    let mut frames_recovered = 0u64;
    let mut truncated_bytes = 0u64;
    let mut next_offset = WalOffset::new(0);
    let mut corruption: Option<Corruption> = None;

    for name in &names {
        let path = name.to_path(dir);
        let bytes = backend.read_all(&path)?;
        let mut pos = 0usize;
        let mut last_good_pos = 0usize;

        loop {
            let tail = &bytes[pos..];
            if tail.is_empty() { break; }                          // 段干净结束
            match msg_proto::peek_total_len(tail) {
                Ok(None) => {
                    // 尾部 < 48 字节或头全但 body 不全 → clean truncation
                    backend.truncate(&path, pos as u64)?;
                    truncated_bytes += tail.len() as u64;
                    break;
                }
                Ok(Some(total)) => {
                    match msg_proto::decode_frame(tail) {
                        Ok(_) => {
                            pos += total; last_good_pos = pos;
                            frames_recovered += 1; next_offset = next_offset.next();
                        }
                        Err(e) => {
                            // CrcMismatch / field 损坏 → 截到 last_good，quarantine，STOP
                            backend.truncate(&path, last_good_pos as u64)?;
                            corruption = Some(Corruption { segment_index: name.index, byte_offset: last_good_pos as u64, kind: CorruptionKind::from(e) });
                            return Ok(RecoveryReport { frames_recovered, segments_scanned: name.index + 1, last_offset: Some(next_offset), truncated_bytes, corruption });
                        }
                    }
                }
                Err(e) => {
                    // BadMagic / BadVersion / NonZeroReserved / BodyTooLarge → corruption，STOP
                    backend.truncate(&path, last_good_pos as u64)?;
                    corruption = Some(Corruption { segment_index: name.index, byte_offset: last_good_pos as u64, kind: CorruptionKind::from(e) });
                    return Ok(RecoveryReport { frames_recovered, segments_scanned: name.index + 1, last_offset: Some(next_offset), truncated_bytes, corruption });
                }
            }
        }
    }
    Ok(RecoveryReport {
        frames_recovered, segments_scanned: names.len() as u32,
        last_offset: if frames_recovered == 0 { None } else { Some(next_offset) },
        truncated_bytes, corruption: None,
    })
}
```

**四种失败模式覆盖**：

| 失败模式 | 触发 | 动作 |
|---|---|---|
| 尾部 < 48 字节（头中途断） | `peek_total_len` `Ok(None)` | 截尾，记 `truncated_bytes`，继续下一段 |
| 头全但 body 不全（帧中途断） | `peek_total_len` `Ok(None)` | 同上 |
| 帧中 CRC/字段损坏 | `decode_frame` `Err(CrcMismatch/...)` | 截到 `last_good_pos`，记 `corruption`，**STOP** |
| 段干净结束 | `tail.is_empty()` | 继续下一段 |

`Wal::open` 调 `recover_dir` 后打开末段（或空目录创建段 0）于 `next_offset` 处续写。

## F. StorageBackend trait + FileIoBackend

trait 6 方法见 §B。要点：

- `sync()` 调 `File::sync_data()`——Linux=fdatasync（Roadmap "fdatasync"），Windows=FlushFileBuffers（== sync_all，Windows 无 fsync/fdatasync 之分）。**一处可移植路径，无 cfg**。
- M1 `MmapBackend` 实现同 trait：`open_segment` 用 `memmap2::MmapOptions::new().map_mut()`；`append` 直接写 mmap 于 `byte_pos` 处；`sync` 调 `mmap.flush_async()`/`flush()`；`size`/`read_all`/`truncate` 同。`Wal` 与 `recover_dir` 只调 trait 方法，swap 是 `Wal::open` 构造处一行 `Box<dyn StorageBackend>` 替换。

## G. 测试矩阵

### 集成测试 `tests/wal.rs`（7 个，均用 `tempfile::TempDir` + 真实 msg-proto 帧）

helper：
```rust
fn frame(seq: u64, body: &[u8]) -> (FrameHeader, Vec<u8>) {
    (FrameHeader::publish(42, 1, 7, seq, body.len() as u32), body.to_vec())
}
```

1. **`append_and_read_back_round_trip`**：空目录 append 1 帧（body `[0xDE,0xAD,0xBE,0xEF]`）+ sync + drop → `Wal::recover` `frames_recovered==1`，读段文件 `decode_frame` 字段+body 一致，`last_offset==Some(WalOffset(1))`。
2. **`batch_commit_atomicity`**：(a) `batch().append(f1).append(f2).append(f3).commit()` → 3 offsets，`metrics.fsyncs==1`，recover 3 帧；(b) 第 2 帧 magic=0（坏头）→ `batch().append(f1).append(bad).append(f3).commit()` 返回 `Err(Encode(BadMagic(_)))`，recover 0 帧（无字节落盘）。
3. **`segment_rolling_at_size_cap`**：`max_segment_bytes: 100`，3 帧各 49 字节（48+1）。append 3 帧 + sync。期望 2 个段文件（`segment-00000000.log` 含 2 帧=98 字节，`segment-00000001.log` 含 1 帧=49 字节），recover 3 帧。
4. **`no_frame_split_across_segment_boundary`**：`max_segment_bytes: 100`。append 1 帧 49 字节（pos 49），再 append 1 帧长 88 字节（49+88=137>100 → 滚动；88<100 可入空段）。期望段 0=49 字节 1 帧，段 1=88 字节 1 帧；recover 2 帧完整。
5. **`recover_after_clean_truncation`**：append 3 帧 + sync + drop。截断 `segment-00000000.log` 到 `2*frame_len + 10` 字节（第 3 帧头被切，剩 10 字节 < 48 → `Ok(None)`）。`Wal::recover` → `frames_recovered==2`，`truncated_bytes==10`，`corruption==None`，段文件恢复后大小=`2*frame_len`。
6. **`recover_after_crc_corruption`**：append 3 帧 + sync + drop。读段文件，翻转帧 1 body 的 1 字节（偏移 `frame_len + HEADER_SIZE + 1`）。`Wal::recover` → `frames_recovered==1`，`corruption==Some(Corruption{segment_index:0, byte_offset:frame_len, kind:CrcMismatch{..}})`，段 0 截到 `frame_len`（1 帧完好）。
7. **`restart_across_process_boundaries`**：open dir A，append 2 帧 + sync + drop。`Wal::open(dir, config)` 再开（内部恢复）→ `report.frames_recovered==2`，`next_offset==WalOffset(2)`。append 1 帧 + sync + drop。`Wal::recover` → `frames_recovered==3`，`last_offset==Some(WalOffset(3))`。

### 单元测试（模块内 `#[cfg(test)] mod tests`）

- `offset.rs`：`WalOffset::new(0).next()==WalOffset(1)`；ordering；`get()` 往返。
- `segment.rs`：`SegmentName::new(0).to_filename()=="segment-00000000.log"`；`from_path` 往返 0/1/99999999；`from_path` 拒 `"foo.log"`/`"segment-0.log"`（宽度错）/`"segment-00000000.bin"`；0..=9 字典序==数值序。
- `config.rs`：`validate()` 拒 `max_segment_bytes=16*1024*1024`（≤ HEADER+MAX_BODY）；接受默认；拒 `fsync_p99_target_us=0`。
- `recovery.rs`：`CorruptionKind::from(ProtoError::CrcMismatch{..})` 正确映射；7 变体全覆盖。

## H. 依赖

**`crates/msg-wal/Cargo.toml`**：

```toml
[package]
name = "msg-wal"
version.workspace = true
edition.workspace = true

[lib]
name = "msg_wal"
path = "src/lib.rs"

[dependencies]
msg-proto = { path = "../msg-proto" }

[dev-dependencies]
tempfile = "3"
```

- `msg-proto`（运行时依赖，非 dev）：WAL 非测试代码调 `encode_frame`（append/commit）+ `peek_total_len`/`decode_frame`（recover_dir）。
- 不引入 `memmap2`（M1）、`crc`（msg-proto 已有 CRC-32C）、`thiserror`（项目约定手写 Display+Error）、`zerocopy`（帧当 `&[u8]` 不透明处理）。
- `tempfile` 仅测试用。

## 验证

执行顺序（Windows PowerShell，cwd `d:\TBOX\brsk-msgx`）：

1. `cargo build -p msg-wal` —— 编译通过。
2. `cargo test -p msg-wal --tests` —— 7 集成测试 + 单元测试全绿。
3. `cargo test --workspace` —— 全 workspace 回归（应从 62 增至 ~70+）。
4. `cargo clippy --workspace --all-targets -- -D warnings` —— 零告警。
5. `cargo fmt --check` —— 格式一致。
6. 手工验证：`Wal::open` 空目录 → 创建 segment-00000000.log；append 几帧 + sync；drop；再 open → recovery report 正确；继续 append 续写。

通过后提交（多 `-m`，不 amend）：
- `git add crates/msg-wal Cargo.lock`
- `git commit -m "feat(msg-wal): M0 WAL skeleton ..." -m "..." -m "..."`
- 推送 origin/main。

## 关键引用文件（已在上下文）

- `d:\TBOX\brsk-msgx\crates\msg-proto\src\frame.rs`：`encode_frame`/`peek_total_len`/`decode_frame`/`encoded_len`/`DecodedFrame` 签名与 `ProtoError` 变体。
- `d:\TBOX\brsk-msgx\crates\msg-proto\src\header.rs`：`FrameHeader`/`HEADER_SIZE=48`/`MAX_BODY_LEN=16*1024*1024`/`publish`/`request`/`reply` 构造器。
- `d:\TBOX\brsk-msgx\crates\msg-transport\src\lib.rs`：内联 error 类型 + crate doc 约定模板。
- `d:\TBOX\brsk-msgx\crates\msg-transport\Cargo.toml` 与 `crates\msg-account\Cargo.toml`：Cargo 模板。
- `d:\TBOX\brsk-msgx\Roadmap.md` §3.1/§4/§9 M1：设计约束与里程碑对照。
