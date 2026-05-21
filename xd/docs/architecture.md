# demo2 详细架构与实现文档

本文档面向当前仓库代码（`demo2` + `tcp_frame_sender`），目标是给出可维护、可联调、可扩展的工程说明。

## 1. 项目定位

- `demo2` 是接收端（上位机）核心工程。
- `tcp_frame_sender` 是测试发送端，用于模拟多传感器 TCP 上报。
- 当前推荐运行形态是“采集服务 + UI 客户端”分离部署：
  - `collector_service`：接入设备、解码、会话处理、事件发布、告警计算、可选入库。
  - `ui_client`：仅消费采集服务推送的 JSON 行流并实时展示。

---

## 2. 分层架构（代码映射）

### 2.1 传输层 `src/transport`

- 核心 trait：`Transport`
  - `connect/read/write_all/close`
- 实现：
  - `TcpTransport`：主动连接（客户端模式）
  - `ConnectedTcpTransport`：使用已 accept 的 socket（服务端接入会话）

职责边界：
- 只处理字节流 I/O，不理解业务字段，不做告警，不做 UI。

### 2.2 协议层 `src/protocol`

- 核心类型：
  - `Frame { request_id, kind, payload }`
  - `FrameCodec` trait
  - `SimpleFrameCodec`
- 能力：
  - 编码/解码
  - 粘包拆包
  - 帧头定位（`MAGIC = 0xAA55`）
  - 长度与 CRC 校验
  - 异常帧容错（错位时按字节滑动恢复）

### 2.3 会话层 `src/session`

- 核心类型：
  - `DeviceSession`
  - `DeviceSessionHandle`
  - `SessionConfig`
- 能力：
  - 连接状态管理：`Disconnected/Connecting/Handshaking/Ready/Degraded/Reconnecting`
  - 命令请求-响应匹配（`pending map`）
  - 命令超时与可重试逻辑
  - 心跳（`0xF0/0xF1`）
  - 读到遥测后发布 `TelemetrySample` 并发送 ACK（`kind = 0x90`）

### 2.4 应用层 `src/app`

- `DeviceManager`：会话编排（启动、调用、停止）
- `CommandService`：统一命令入口
- `AlarmService`：阈值告警状态机（触发/恢复）

### 2.5 事件与状态层 `src/bus`

- `EventBus`：进程内发布订阅（`tokio::broadcast`）
- `Store`：设备快照缓存（`RwLock<HashMap<...>>`）
- 事件类型：
  - `ConnStateChanged`
  - `TelemetryUpdated`
  - `TelemetrySample`
  - `AlarmRaised/AlarmCleared`
  - `CommandResult`
  - `Log`

### 2.6 表现层 `src/bin/ui_client.rs`

- 从 `collector_service` 的 `19011` 拉取 JSON 行流。
- 展示 10 个传感器窗口，支持拖动和 `Ctrl + 滚轮` 缩放。
- 每路图仅保留最近 `30s` 数据。
- 当采样间隔过大时断线绘制（避免“长时间空白自动连线”）。

---

## 3. 进程拓扑与端口

### 3.1 分离部署（推荐）

- `collector_service`
  - 设备接入：`127.0.0.1:19010`
  - UI 数据流：`127.0.0.1:19011`
  - 健康检查：`127.0.0.1:19012`
- `ui_client`
  - 连接 `19011` 消费 UI 数据
- `tcp_frame_sender/sender_1min`
  - 连接 `19010`，按传感器分连接上报

### 3.2 一体化调试（兼容）

- `acceptor_ui`：单进程接入 + UI，不依赖 `collector_service`。

---

## 4. 帧格式与协议约定

### 4.1 二进制帧结构

- `magic: u16`（固定 `0xAA55`）
- `body_len: u16`
- `request_id: u64`
- `kind: u8`
- `payload: [u8; N]`
- `crc16: u16`（对 `request_id + kind + payload` 计算）

### 4.2 关键 `kind`

- `0x34`：上报遥测（sender 使用）
- `0x90`：ACK（collector/session 回包）
- `0xF0`：心跳请求
- `0xF1`：心跳响应
- `0x81`：示例命令响应（测试中 mock 设备使用）

### 4.3 遥测 payload 约定

当前字符串格式：

`sid=<sensor_id>,value=<float>`

示例：

`sid=3,value=47.381`

---

## 5. 端到端数据流（Sender -> UI）

1. `sender_1min` 启动 10 个线程，每个传感器一个 TCP 连接（错峰 0.5s 接入）。
2. `collector_service` accept 新连接，为每个连接创建一个 `DeviceSession`。
3. `DeviceSession` 从字节流解码 `Frame(kind=0x34)`，解析 `sid/value`。
4. 会话层发布 `AppEvent::Device(TelemetrySample)` 到 `EventBus`，并发回 ACK。
5. `run_alarm_forwarder` 订阅 `TelemetrySample`，调用 `AlarmService::evaluate_sample`，按状态跃迁发布告警事件。
6. `run_persistence_forwarder` 订阅事件并写 PostgreSQL（若启用 DB）。
7. `run_ui_forwarder` 将 `TelemetrySample` 序列化成 JSON 行，广播给 UI Feed 客户端。
8. `ui_client` 接收后按 `sensor_id` 入队，绘制最近 30s 曲线。

---

## 6. 会话模型细节

### 6.1 状态机

- `Disconnected -> Connecting -> Handshaking -> Ready`
- 异常或超时后进入 `Degraded/Reconnecting`（取决于配置）

### 6.2 当前 collector 场景配置

`collector_service` 中会话使用：

- `enable_heartbeat = false`
- `reconnect_enabled = false`

原因：
- collector 处理的是“已 accept 的被动连接”，断开后等待设备端重连即可。

### 6.3 快速失败策略

- 写失败：当前命令立即失败并结束该会话循环。
- 读到 EOF：认为连接关闭，结束会话。
- pending 请求：超时后按 `idempotent + retry_policy` 决定重试或失败返回。

---

## 7. 告警逻辑（`src/app/alarm_service.rs`）

### 7.1 规则结构

- `AlarmRule`：
  - `high/high_clear`
  - `low/low_clear`
  - `level`
  - `name`

### 7.2 状态结构

- `(device_id, sensor_id) -> SensorAlarmState { high_active, low_active }`

### 7.3 判定机制

- 高报触发：`value >= high`
- 高报恢复：`value <= high_clear`
- 低报触发：`value <= low`
- 低报恢复：`value >= low_clear`

使用“触发阈值/恢复阈值分离”的滞回机制，减少抖动反复告警。

---

## 8. 数据存储与内存模型

### 8.1 内存数据

- `Store`：每设备最新快照（非全量历史）
- `ui_client`：每传感器 `VecDeque<[t, value]>`，按 30s 滑窗裁剪
- `EventBus`：有界广播通道，慢消费者会丢历史消息

### 8.2 PostgreSQL 持久化

由 `collector_service` 启动时自动建表：

- `telemetry_samples`
- `alarm_events`
- `system_events`

并创建时间与设备维度索引。

### 8.3 数据库位置说明

- 若走 `docker compose`，数据库在容器中（默认 `postgres` 服务）。
- 连接串由 `config.toml` 的 `collector.pg_dsn` 指定。

---

## 9. 并发与背压

### 9.1 并发结构

- 每个设备连接 -> 一个 `DeviceSession`（Tokio 任务）
- UI Feed -> `broadcast::Sender<String>`
- DB 写入 -> 独立 `mpsc` 写线程

### 9.2 背压点

- `EventBus` 有界；消费不及时会 `Lagged`
- UI 本地通道 `sync_channel` 有界；满时丢样并计数
- DB 通道有界；写入慢会堆积或丢弃（调用方可按需求增强处理）

---

## 10. 配置与运行

### 10.1 配置文件

- `config.toml`（可从 `config.toml.example` 复制）
- 关键项：`ingress_addr/ui_feed_addr/health_addr/pg_dsn`

### 10.2 推荐启动顺序

1. `collector_service`
2. `ui_client`
3. `sender_1min` 或 `sender_stress_report`

### 10.3 常用命令（PowerShell）

```powershell
# 可选：启动 PostgreSQL
docker compose up -d postgres

# 启动采集服务
cargo run --bin collector_service

# 启动 UI
cargo run --bin ui_client

# 启动发送端（在 tcp_frame_sender 目录）
cargo run --bin sender_1min
```

---

## 11. 常见故障排查

### 11.1 `os error 10061`（连接被拒绝）

含义：目标端口没有监听或被防火墙拦截。

排查顺序：
1. 确认 `collector_service` 是否已启动。
2. 核对 sender 连接地址是否是 `19010`。
3. 核对 UI 连接地址是否是 `19011`。
4. 在本机检查端口监听状态（`netstat -ano | findstr 1901`）。

### 11.2 UI 无数据

1. 看 UI 顶部 `status` 是否显示 `connected feed: 127.0.0.1:19011`。
2. 看 collector 的 `samples_rx` 是否增长。
3. 检查 sender 是否打印连接成功和周期发送日志。

### 11.3 数据库相关

若 PostgreSQL 不可用，collector 会降级为“无持久化模式”继续运行（并输出 warning）。

---

## 12. 扩展指南

### 12.1 新增帧类型（不影响现有功能）

建议流程：
1. 在协议层约定新 `kind` 与 payload 格式。
2. 在 `session::connection_loop` 增加对应分支处理。
3. 新增/复用 `DeviceEvent` 事件类型。
4. 在 `collector_service` 的 forwarder 中选择性转发到 UI/DB。
5. 为新路径补充单测与 e2e。

这样可以做到“新增能力不破坏既有链路”。

### 12.2 一帧多组传感器数据

可选两种方案：
- 方案 A：payload 内部承载数组（推荐，改动小）
- 方案 B：定义新 kind，单帧多子包

落地时建议在会话层解析后拆成多条 `TelemetrySample` 事件，UI 与告警层无需大改。

---

## 13. 测试体系

- 单元测试：
  - 协议层粘包拆包、CRC、异常恢复
  - 告警阈值触发/恢复
- 集成测试：
  - `tests/session_sender_to_ui_e2e.rs`
  - 覆盖会话、命令、遥测事件流
  - 可设置 `SHOW_UI=1` 观察测试 UI

---

## 14. 当前成熟度与上线建议

当前代码适合：
- 开发联调
- 功能演示
- 小规模 PoC

要进入企业生产，建议补齐：
1. 完整鉴权与链路加密（TLS/证书/密钥管理）。
2. 更严格的资源隔离与限流（连接数、每设备速率、异常流量保护）。
3. 持久化可靠性增强（批写、重试、死信/补偿）。
4. 全链路可观测性（metrics、trace_id、报警看板）。
5. 灰度发布与回滚流程。

---

## 15. 总结

当前 `demo2` 已从“围绕 socket 直接处理”演进为“围绕会话 + 事件 + 应用服务”的结构：

- 传输层、协议层、会话层、应用层、UI 层职责清晰；
- 会话层统一承载连接生命周期与命令匹配；
- UI/告警/持久化通过 EventBus 解耦扩展；
- 在不破坏主链路前提下，可以持续增加新帧类型与业务能力。

