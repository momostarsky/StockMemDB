# brsk-msgx 二进制消息转发平台 — 技术路线规划

| 项目 | 内容 |
|---|---|
| 文档版本 | v0.2（草案，待会签） |
| 编写日期 | 2026-08-28 |
| 文档状态 | 待评审会签 |
| 目标平台 | Linux（裸金属优先），Rust 实现 |

---

## 1. 项目概述

面向金融计算平台的高性能二进制消息转发系统：支持问答（REQ/REP）与发布订阅（PUB/SUB）两种模式、主从/集群高可用、端到端消息不丢失、灵活过滤、失败重试，并通过 gRPC / FIX 对外接入，通过 WebSocket / Redis Stream 等对监控平台推送。

### 1.1 需求对照表

| # | 需求 | 方案落点 | 章节 |
|---|---|---|---|
| 1 | Rust 实现，Linux 运行 | Rust + tokio（接入层）/ monoio（数据面），musl 静态编译 | §2、§3 |
| 2 | 主从切换或单机，Raft 协议 | openraft，单机模式（Raft off）/ 集群模式同一份代码配置切换 | §3、§9-M4 |
| 3/10 | 消息不丢失（MMP 持久化 / 时序库） | mmap WAL（理解为 MMP）+ group fsync + CRC；冷数据归档 ClickHouse/TDengine | §4 |
| 4 | 转发支持过滤 | 两级过滤（Header 索引 → 属性谓词）+ WASM 插件过滤消息体内部属性 | §3、§5 |
| 5 | DPDK 或接近技术 | Transport trait：v1 io_uring TCP → v1.5 AF_XDP → DPDK+shm（可选） | §6、§7 |
| 6 | 失败重试，次数+时间限制 | 指数退避 + max_attempts + TTL deadline，超限进死信（DLQ） | §4 |
| 7 | 问答 / 发布订阅模式 | REQ/REP：correlation_id + 超时；PUB/SUB：消费者组 + offset 回溯 | §3 |
| 8 | C#/Java gRPC + FIX 接入 | tonic + 统一 proto IDL；FIX 4.4 网关独立进程 | §3、§9-M3/M6 |
| 9 | WebSocket / Redis Stream 推监控 | WebSocket（面板）+ Redis Stream（平台）为主，Kafka/Prometheus/OTel 为辅 | §8 |

---

## 2. 总体架构

```
                    ┌─────────────────────────────────────────────┐
 接入层(南向)        │              消息内核 (msg-core)              │        推送层(北向)
┌───────────────┐   │  ┌─────────┐  ┌──────────┐  ┌────────────┐  │   ┌──────────────────┐
│ gRPC (tonic)  │──?│  │ 路由/过滤 │  │ Raft 复制 │  │ 重试/死信   │  │──?│ WebSocket(监控台) │
│ FIX 网关      │   │  │ 引擎     │─?│ (openraft)│─?│ 队列        │  │   │ Redis Stream     │
│ Transport     │   │  └─────────┘  └──────────┘  └────────────┘  │   │ Kafka(可选归档)   │
│ (TCP/AF_XDP/  │   │  ┌──────────────────────────────────────┐   │   │ Prometheus/OTel  │
│  DPDK+shm)    │   │  │ WAL 持久化 (mmap + group fsync + CRC) │   │   └──────────────────┘
└───────────────┘   │  └──────────────────────────────────────┘   │
                    └─────────────────────────────────────────────┘
```

核心思想：**一切以 WAL 为准**。消息先落盘（fsync）再确认（ACK），主从通过 Raft 复制 WAL，消费端按 offset ACK，形成端到端不丢失链路。

### 2.1 工程结构（Cargo workspace）

```
msg-proto          # 二进制协议 v1 + protobuf IDL（三语言存根生成）
msg-core           # REQ-REP 与 PUB-SUB 语义、monoio 数据面
msg-filter         # 两级过滤引擎、Schema 属性抽取、WASM 插件宿主（wasmtime）
msg-wal            # mmap WAL：segment 管理、group commit、CRC、崩溃恢复
msg-raft           # openraft 集成，log storage 复用 WAL
msg-transport      # Transport trait + 帧句柄 + Mock/TCP/AF_XDP 实现
msg-gateway-grpc   # gRPC 接入层（tokio + tonic，与数据面 SPSC ring 互连）
msg-gateway-fix    # FIX 4.4 网关（独立进程）
msg-monitor        # WebSocket / Redis Stream / Prometheus 指标
```

---

## 3. 技术选型

| 需求 | 选型 | 说明 |
|---|---|---|
| 语言/平台 | Rust + monoio（数据面）/ tokio（接入层） | thread-per-core；monoio 支持 epoll fallback |
| 主从/集群 | **openraft** | 上层封装完整；单机/集群模式热切换 |
| 持久化 | **mmap WAL**（memmap2 + 自研 segment） | 顺序追加、group commit、CRC32C、段回收 |
| 过滤 | 两级过滤：Header 索引 + Schema 属性抽取 + CEL；扩展层 WASM 插件 | 详见 §5；v1 不做插件 |
| 高性能接入 | Transport trait 可插拔：io_uring TCP → AF_XDP → DPDK+shm | 见 §6、§7 |
| 重试 | 指数退避 + max_attempts + TTL，超限进 DLQ | 双限制同时生效 |
| 消息模式 | REQ/REP（correlation_id+超时）、PUB/SUB（消费者组+offset 回溯） | 同一套存储内核 |
| gRPC | tonic + protobuf IDL 单仓 | C#（Grpc.Net）/ Java（grpc-java）存根生成 |
| FIX | FIX 4.4 网关独立进程（参考 fix-rs / quickfix-rs） | 内部转二进制协议，会话状态不污染核心 |

---

## 4. 消息不丢失 — 端到端设计（最高优先级）

四个环节全部不可省，外加兜底：

1. **写入端**：生产者 → 服务端写 WAL → `fdatasync` 成功 → 回 ACK。提供 `acks=leader` / `acks=quorum` 两级语义，金融场景默认 quorum。
2. **复制端**：Raft log 与 WAL 合一（openraft log storage 直接落 mmap 段文件），多数派落盘才算 committed。
3. **消费端**：按 offset 消费、显式 ACK 后推进游标，断线从上次 ACK 位重放 → 至少一次。
4. **幂等去重**：消息带 `producer_id + seq`，服务端维护去重窗口 → 有效 exactly-once。
5. **兜底**：段文件 CRC 校验、启动崩溃恢复扫描、watchdog 监控 fsync P99、disk-full 熔断（拒绝写入而非静默丢弃）。

---

## 5. 过滤引擎与插件体系

### 5.1 两级过滤架构（热路径性能关键）

插件过滤不逐订阅者执行，消息到达后的过滤流水线：

```text
消息到达
  ├─ 第一级（零成本）: Topic 前缀树 + Header 索引字段匹配 → 淘汰绝大多数不相关订阅
  ├─ 属性抽取（每条消息仅一次）: 按 Header.schema_id 从 Schema 注册表取抽取描述符
  │   → body 指定偏移字段 → ExtractedAttrs（缓存进消息元数据）
  └─ 第二级（仅命中订阅者）: 编译期谓词 over ExtractedAttrs
      - CEL 谓词: 订阅时编译为匹配树
      - WASM 插件: AOT 实例, eval(attrs_ptr, len) -> u8
```

前提：消息 Header 增加 `schema_id`（+ 可选 `attr_offset`）字段；Schema 注册表管理各业务消息体字段描述符。第一级为纯 Header 解析，纳秒级。

### 5.2 插件形态对比

| 形态 | 性能 | 安全隔离 | 谁能写 | 评估 |
|---|---|---|---|---|
| **WASM 插件**（wasmtime AOT） | eval ~1-10μs | ? 崩溃/死循环被沙箱拦截 | Rust/C#/Java/Go | **金融场景首选** |
| C-ABI 动态库（extern "C"） | ~100ns | ? 插件崩 = 内核进程崩 | Rust/C/C++ | 仅限内部白名单 |
| 嵌入式脚本（CEL/Rhai/Lua） | ~1μs | ? 无副作用沙箱 | 业务人员 | CEL 为第一级谓词语言 |

注意：Rust 无稳定 ABI，纯 Rust dylib 直接 libloading 跨编译器版本不兼容，必须走 C-ABI 或 abi_stable——"进程内随意加载 Rust 插件"不成立，强化 WASM 方案。

### 5.3 插件契约草案

```rust
/// 插件 = 抽取器 + 谓词（WASM 导出约定: plugin_init / plugin_eval / plugin_deinit）
pub trait BodyFilterPlugin: Send + Sync {
    fn name(&self) -> &str;
    fn init(&self, config: &[u8]) -> Result<(), PluginError>;   // 订阅时执行一次
    /// 返回: 0=不匹配 1=匹配 2=错误(按订阅策略处理)
    fn eval(&self, attrs: &ExtractedAttrs) -> u8;
    fn deinit(self);
}
```

WASM 宿主要求：wasmtime AOT 编译、每插件 fuel 限额（防死循环）、内存上限、eval 传线性内存指针零拷贝。

### 5.4 失败语义（金融平台硬性要求）

插件 eval 出错或超时 → **按"不匹配"处理并计数告警**（宁可少投递，由死信/监控补偿），绝不允许插件故障阻塞 WAL 主链路。插件管理接口：上传/版本/灰度/禁用。

### 5.5 落地排期

- v1（M1）：不做插件。第一级 Header 过滤 + schema_id 属性抽取 + CEL 已覆盖约 80% 场景
- M5.5：WASM 插件宿主上线（见 §9）
- C-ABI dylib 极致性能选项放 M7 后再议（需进程级隔离配套）

---

## 6. Transport trait — 传输层契约（先定契约，再填实现）

### 6.1 三种实现

| 实现 | 进程形态 | 帧来源 | 缓冲区生命周期 |
|---|---|---|---|
| `IoUringTcpTransport` | 进程内 | TCP 流 → 长度前缀拆包 | 堆内存，用完释放 |
| `AfXdpTransport` | 进程内 | XDP 四组 ring | UMEM 帧，归还 FILL/COMPL ring |
| `DpdkShmTransport` | 跨进程（C++ 数据面） | 共享内存 rte_ring | mbuf 指针，归还 mempool |

### 6.2 设计决策（四条纪律）

1. **统一帧单元**：trait 层"一帧 = 一个完整应用层消息"。TCP 拆包在实现内部完成；核心只见消息帧。
2. **零拷贝所有权显式化**：帧句柄提供显式 `release()`；`Drop` 仅兜底并告警计数，禁止靠 Drop 管理热路径。
3. **批量接口**：所有收发 batch 粒度（底层 ring 均为批量操作）。
4. **线程归属**：传输实现自持线程（AF_XDP busy-poll 绑核 / TCP 用 runtime），核心经 SPSC ring 交换帧。

### 6.3 接口草案

```rust
pub trait Transport: Send + Sync + 'static {
    fn info(&self) -> &'static TransportInfo;      // 名称、能力位、统计
    fn start(self: Arc<Self>, ctx: TransportCtx) -> TransportHandle;
}

pub struct TransportCtx {
    pub tx_to_core: spsc::Producer<Inbound>,       // 传输 → 核心（批量）
    pub rx_from_core: spsc::Consumer<Outbound>,    // 核心 → 传输（批量）
    pub events: mpsc::Sender<TransportEvent>,      // 健康/丢包/降级事件
}

pub struct Inbound  { pub frame: RxFrame, pub meta: RxMeta }  // RxMeta: HW时间戳/RSS hash/队列号
pub struct Outbound { pub frame: TxFrame }

pub struct RxFrame(/* 各实现私有 */);
impl RxFrame {
    pub fn payload(&self) -> &[u8];
    pub fn release(self);        // AF_XDP→UMEM / DPDK→mempool / TCP→堆
}

pub trait FrameAlloc: Send + Sync {
    fn alloc(&self, len: usize) -> Option<TxFrame>;  // None = 传输侧背压
}
impl TxFrame {
    pub fn payload_mut(&mut self) -> &mut [u8];
    pub fn send(self);
}

pub struct TransportInfo {
    pub name: &'static str,
    pub caps: TransportCaps,                 // ZERO_COPY_RX | HW_TIMESTAMP | BUSY_POLL ...
    pub metrics: &'static TransportMetrics,  // ring 满、丢帧计数 → Prometheus
}
```

两个包装器（不进 trait）：

- `DegradingTransport`：监听 TransportEvent，主传输异常时热切 fallback（TCP），满足旁路故障不中断的金融硬要求。
- `MockTransport`：进程内环回，Windows/CI 上跑全链路测试。

### 6.4 契约买到的能力

1. 降级即插即用；2. Windows/CI 可测；3. 三实现同一核心压测 A/B 可比；4. C++ DPDK 对核心只是普通 Transport 实现（版本校验头、跨进程细节锁在实现内部）。

---

## 7. 高性能路径评估

### 7.1 成熟度分层（AF_XDP 争议的澄清）

| 层 | 成熟度 | 依据 |
|---|---|---|
| 内核 AF_XDP 机制 | ? 成熟 | 4.18（2018）进主线，UAPI 稳定；Suricata 等生产级应用；mlx5/i40e/ixgbe 零拷贝路径久经考验 |
| libbpf XSK 支持 | ? 成熟 | 官方维护 |
| `xsk-rs` Rust 封装 | ?? 不成熟 | <1.0、使用者少、无大规模生产背书——**不成熟的只是这一层** |

风险控制：AF_XDP 内核接口面小（socket+setsockopt、四组 ring mmap、一段 XSKMAP BPF），`xsk-rs` 可替换为自研 libbpf-sys 薄封装（千行级）。自研面小于 DPDK 跨进程方案（mbuf 所有权协议 + 版本校验 + 跨进程恢复）。

### 7.2 性能量化估算

前提：NVMe、10G/25G 网、8~16 核、平均消息 256~512B。

| 场景 | msg/s | 字节吞吐 | 说明 |
|---|---|---|---|
| 10G 线速 @512B 帧 | ~2.4M | 1.2 GB/s | 物理天花板 |
| 25G 线速 @512B 帧 | ~6M | 3 GB/s | 物理天花板 |
| monoio + io_uring TCP 纯转发 | 2~5M | ~1 GB/s | 内核栈 ~1M pps/核；8 核可逼近 10G 线速 |
| + WAL mmap 落盘（group commit 256条/fsync，fsync≈50μs） | 2~4M | 0.6~1.2 GB/s | group commit 是关键 |
| + Raft 三副本 quorum | 1~2M | ~0.5 GB/s | 多一跳网络 + 远端 fsync |
| + 过滤/去重/重试 | 打 8 折 | — | 前缀树匹配 ~100ns/订阅者 |

```text
持久化吞吐上限 ≈ group_commit批量 / fsync延迟
延迟对比: TCP 30~60μs P99 | AF_XDP 5~15μs | DPDK 2~10μs
```

**结论**：平均消息 256B~1KB、10G 网络下，monoio TCP 已能打满网卡线速（~2M msg/s、1GB/s 级），旁路主要收益是降延迟。旁路真正的赢面：25G+、消息 <128B、延迟 SLA <10μs、pps >5M。

**纪律：以上为估算，M0 spike 必须在目标生产硬件实测。**

---

## 8. 监控推送方式

| 方式 | 适用场景 | 建议 |
|---|---|---|
| WebSocket | 实时监控面板、低延迟推送 | ? 第一优先 |
| Redis Stream | 已有 Redis 生态的监控平台 | ? 第一优先 |
| Kafka | 长期审计、大数据侧消费 | M6 加，金融审计几乎必备 |
| Prometheus + OpenTelemetry | 指标/链路监控（非消息本身） | ? 必做，Grafana 出图 |
| gRPC server-streaming | 内部 Java/C# 监控程序直接订阅 | 复用现有 gRPC 端口 |
| NATS / ZeroMQ | 轻量场景 | 可选，不首推 |
| Syslog / Fluent Bit | 运维日志侧 | 可选 |

---

## 9. 实施路线（先后顺序）

### M0 — 骨架与协议 + 性能 Spike
- Cargo workspace 划分（§2.1）；二进制协议 v1（magic/version/flags/topic-id/producer_id/seq/correlation_id/长度/CRC）
- proto IDL + 三语言存根生成脚本；Linux CI（fmt/clippy/test）
- **性能 Spike（目标生产硬件）**：`bench-tcp`（monoio io_uring 收发 pps）+ `bench-wal`（mmap WAL group commit，扫批量 64/256/1024 × fsync 延迟曲线）
- Spike 结果决定 AF_XDP 优先级

### M1 — 消息内核（最重要的地基）
- mmap WAL：segment 管理、group commit、CRC、崩溃恢复
- 路由引擎：Topic → 订阅者，两级过滤（Header 索引 + schema_id 属性抽取 + CEL 谓词）
- REQ/REP 与 PUB/SUB 语义（IoUringTcpTransport）
- 交付：单机压测基线（criterion + bench client）

### M2 — 可靠性闭环
- ACK 语义（leader/quorum）、消费 offset 管理、幂等去重窗口
- 重试队列：指数退避、max_attempts、TTL deadline、DLQ + 死信查询
- 磁盘满/校验错等故障路径 + 混沌测试

### M3 — 对外接入层
- gRPC（tonic）：生产/消费/管理三类接口；C# 与 Java SDK 跑通
- WebSocket 监控推送 + Prometheus 指标；Redis Stream 适配器

### M4 — 主从与集群
- openraft：单机模式（Raft off）/ 3/5 节点集群模式，配置切换
- Leader 切换时消费游标与 DLQ 状态的元数据复制
- 脑裂演练：kill leader、网络分区（确定性仿真测试）

### M5 — 极致性能（内核稳定后）
- AF_XDP（feature-gated）：busy-poll 绑核线程 + SPSC ring 接 monoio；裸机专项验证（ring 打满、驱动 reset、长时 soak）
- `DegradingTransport` 上线：旁路异常自动降级 TCP
- C++ DPDK + shm 作为备选实验分支（仅当有存量 C++ 资产或极限延迟指标）
- 目标：单机 P99 < 50μs（旁路后 < 15μs），持久化吞吐 ≥ 数百万 msg/s

### M5.5 — 过滤插件体系（WASM）
- wasmtime AOT 宿主、BodyFilterPlugin 契约落地、插件管理接口（上传/版本/灰度/禁用）
- ExtractedAttrs 抽取框架与 Schema 注册表对接；失败语义（按不匹配处理 + 告警计数，见 §5.4）

### M6 — FIX 网关与时序归档
- FIX 4.4 会话引擎（独立进程）映射到二进制协议 topic
- WAL 冷段归档 ClickHouse/TDengine（时序查询与审计）
- Kafka 审计推送（如监控平台需要）

### M7 — 生产化
- cargo-fuzz（协议解析）、loom/TSAN 并发验证
- systemd/k8s 部署、配置热更、灰度开关
- 运维手册：扩容、副本修复、死信处理 SOP

**排序理由**：先 WAL 内核再集群（Raft 只是 WAL 的复制器）；性能优化放后（先正确再快）；FIX 是网关插件不阻塞主线。

---

## 10. 风险清单

| # | 风险 | 缓解 |
|---|---|---|
| 1 | Rust DPDK 生态不成熟 | AF_XDP 替代 + Transport trait 可插拔 + TCP 降级兜底 |
| 2 | `xsk-rs` 不成熟 | 接口面小，可替换为 libbpf-sys 自研薄封装；feature-gate + 不达标不切默认 |
| 3 | fsync 延迟抖动 | group commit + NVMe 必配；fsync P99 作为核心 SLI 进 Prometheus |
| 4 | openraft 学习曲线 | M4 预留确定性仿真测试周期；raft-rs（tikv）备选 |
| 5 | FIX 会话状态复杂 | 独立进程，崩溃不影响核心链路 |
| 6 | 性能估算偏差 | M0 spike 在目标硬件实测，数据驱动决策 |
| 7 | 开发机为 Windows，AF_XDP 仅 Linux 裸机可测 | MockTransport 保 CI 全链路；裸机专项验证环境提前申请 |
| 8 | 插件故障影响主链路 | WASM 沙箱隔离 + fuel/内存限额 + 失败按不匹配处理并告警（§5.4） |

---

## 11. 会签

> 本文档为 v0.2 草案。会签通过后进入 M0 实施；对 §5 过滤插件契约、§6 传输层契约、§9 里程碑顺序、§7 性能结论的任何修改需更新版本号并重新会签。

### 11.1 待确认事项（会签前请逐项确认）

| # | 事项 | 影响 | 待填 |
|---|---|---|---|
| 1 | 生产网卡型号/带宽（10G/25G？mlx5/i40e/ixgbe？） | 决定 AF_XDP 零拷贝可行性与优先级 | |
| 2 | 消息尺寸分布与峰值 msg/s 目标 | 决定是否必须旁路 | |
| 3 | 延迟 SLA（P99 目标值） | <10μs 则旁路必选 | |
| 4 | 部署形态（裸金属/VM/云） | VM 下 AF_XDP 仅 generic 模式 | |
| 5 | ACK 语义默认值（leader/quorum） | 吞吐与安全的权衡 | |
| 6 | 存量 C++ DPDK 资产是否存在 | 决定 DpdkShmTransport 是否立项 | |
| 7 | 集群规模（单机/3 节点/5 节点起步） | M4 范围 | |
| 8 | 是否需要跨语言编写过滤插件（C#/Java 等） | 决定 WASM 插件宿主优先级 | |

### 11.2 会签记录

| 角色 | 姓名 | 意见（同意/修改/否决） | 日期 | 签名 |
|---|---|---|---|---|
| 架构负责人 | | | | |
| 消息内核/存储 | | | | |
| 传输层/高性能 | | | | |
| 接入层（gRPC/FIX/SDK） | | | | |
| 运维/SRE | | | | |
| 测试/质量 | | | | |
| 项目/业务代表 | | | | |

### 11.3 修订记录

| 版本 | 日期 | 修订内容 | 修订人 |
|---|---|---|---|
| v0.1 | 2026-08-28 | 初稿：技术路线、Transport 契约、里程碑、风险清单 | |
| v0.2 | 2026-08-28 | 新增 §5 过滤引擎与插件体系（两级过滤、WASM 契约、失败语义、M5.5）；章节重编号；风险清单补插件项 | |
