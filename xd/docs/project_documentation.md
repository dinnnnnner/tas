# demo2 项目详细文档

本文档基于当前仓库源码整理，面向开发、联调、测试和后续维护。项目根目录为 `xd/`，Rust 包名为 `demo2`。

## 1. 项目概览

`demo2` 是一个面向上位机采集与展示的 Rust MVP 工程。它把设备接入、协议解码、会话管理、事件分发、告警、数据库持久化和 UI 展示拆分为多个模块。

当前主要运行形态有两种：

1. 推荐联调形态：`collector_service` 作为采集服务运行，`ui_client` 作为 UI 客户端连接采集服务。
2. 当前 UI 入口的实际行为：`ui_client` 内部也通过 `#[path = "collector_service.rs"]` 嵌入并启动了一份 collector runtime，同时再连接 `127.0.0.1:19011` 的 UI feed。单独启动 `collector_service` 后再启动 `ui_client` 时，需要注意端口占用问题。

项目支持的接入方式：

- TCP legacy 二进制帧接入。
- 串口 legacy 帧接入。
- 串口 demo 协议接入。
- 串口 SENT 模式接入。
- CAN / CAN FD 接入，当前实现针对 TSMaster / TC1012 运行环境。

项目支持的输出与观测方式：

- UI JSON 行流：采集服务向 UI 客户端广播遥测、告警、状态消息。
- PostgreSQL 持久化：写入遥测、告警和系统事件。
- 健康检查端口：提供 `/health` 和 `/ready`。
- eframe / egui 桌面 UI：实时曲线、告警状态、CAN 回放、告警记录查看。

## 2. 技术栈

- Rust edition 2024。
- Tokio：异步 TCP、任务调度、通道。
- eframe / egui / egui_plot：桌面 UI 与曲线绘制。
- serialport：串口访问。
- tokio-postgres：PostgreSQL 持久化。
- libloading + windows-sys：动态加载 TSMaster DLL 并调用 CAN API。
- serde / serde_json / toml：配置和 UI feed 消息序列化。
- biquad：数据库写入前的可选 Butterworth 低通滤波。
- tracing：运行日志。

## 3. 顶层目录结构

```text
xd/
  Cargo.toml
  Cargo.lock
  README.md
  config.toml.example
  config.toml
  docker-compose.yml
  docs/
    architecture.md              旧架构文档，当前存在中文编码损坏
    project_documentation.md     本文档
  exports/                       CAN/回放导出文件示例
  scripts/
    run_demo2.ps1                Windows 一键启动脚本，当前中文提示存在编码损坏
  src/
    lib.rs
    app/
    bus/
    db/
    domain/
    ingress/
    protocol/
    session/
    signal/
    transport/
    bin/
```

`target/` 与 `target-codex.../` 是构建产物目录，不属于源码维护重点。

## 4. 源码模块说明

### 4.1 `src/lib.rs`

库入口，导出项目各核心模块：

- `app`
- `bus`
- `db`
- `domain`
- `ingress`
- `protocol`
- `session`
- `signal`
- `transport`

### 4.2 `src/domain`

定义跨模块共享的领域类型：

- `DeviceId = String`
- `RequestId = u64`
- `RetryPolicy`
- `CommandKind`
- `Command`
- `Response`
- `ConnState`
- `DeviceSnapshot`
- `AlarmLevel`
- `AlarmEvent`

连接状态机枚举：

```text
Disconnected
Connecting
Handshaking
Ready
Degraded
Reconnecting
```

`CommandKind` 当前内置：

- `ReadParam` -> `0x01`
- `WriteParam` -> `0x02`
- `Control` -> `0x03`
- `Custom(u8)` -> 自定义 kind

### 4.3 `src/bus`

事件总线与最新状态缓存：

- `TelemetrySourceKind`：标识遥测来源。
- `DeviceEvent`：设备级事件。
- `AppEvent`：应用级事件封装。
- `EventBus`：基于 `tokio::broadcast` 的进程内发布订阅。
- `Store`：基于 `RwLock<HashMap<...>>` 的设备快照缓存。

`TelemetrySourceKind` 当前包括：

- `Unknown`
- `SerialDemo`
- `SerialSent1`
- `SerialSent2`
- `SerialSent3`
- `CanAxis`
- `CanSent`
- `TcpFrame`
- `FrameStream`

`DeviceEvent::TelemetrySample` 是主数据流事件，字段包括：

```text
device_id
sensor_id
t_sec
value
req_id
alarm_bit
source_kind
```

注意：`EventBus` 是有界广播通道，慢消费者会收到 `Lagged`，旧消息可能被跳过。

### 4.4 `src/protocol`

协议编解码模块，包含两条主要协议线：

1. `SimpleFrameCodec`：TCP / legacy 串口二进制帧。
2. `SerialDemoCodec`：demo 串口协议，位于 `protocol/demo_serial.rs`。
3. `SentFrameCodec`：SENT 10 字节帧。

#### SimpleFrame 帧结构

```text
magic:      u16 = 0xAA55
body_len:   u16
request_id: u64
kind:       u8
payload:    [u8; N]
crc16:      u16
```

CRC 计算范围为 `request_id + kind + payload`。解码器会处理粘包、拆包、错位恢复、长度上限和 CRC 校验。

常用 kind：

- `0x34`：遥测上报。
- `0x90`：ACK。
- `0xF0`：心跳请求。
- `0xF1`：心跳响应。

legacy 遥测 payload 当前采用文本格式：

```text
sid=<sensor_id>,value=<float>
```

示例：

```text
sid=3,value=47.381
```

#### SENT 帧结构

SENT 帧长度固定为 10 字节：

```text
sync marker: 0xF0
status:      4-bit
channel_1:   12-bit
channel_2:   12-bit
crc:         4-bit
pause:       4-bit
```

`SentFrameCodec::try_decode` 会查找同步标记并验证 nibble CRC。`pause` 用于区分当前帧语义：

- `0x1`：SENT1，发布 sensor 0 / 1。
- `0x6`：SENT2，发布 sensor 2 / 3。
- `0xB`：SENT3，发布 sensor 4。

#### demo 串口协议

`SerialDemoCodec` 支持命令响应和固定长度数据流：

- 帧头：`0x7B`
- 流命令：`0x07`
- 数据流总长度：`0x2004`
- 数据体长度：8192 字节
- group 大小：32 字节
- group 数量：256

一个 group 包含 4 路通道，每路通道解析为三轴数据和 alarm bit。当前采集端主要使用 `ch1` 的 x/y/z 发布 sensor 0/1/2，并根据 `ch1.alarm` 发布 demo 告警事件。

### 4.5 `src/transport`

传输抽象层，只负责字节流 I/O，不理解业务协议。

核心 trait：

```rust
#[async_trait]
pub trait Transport: Send + Sync {
    async fn connect(&mut self) -> Result<(), TransportError>;
    async fn read(&mut self, dst: &mut BytesMut) -> Result<usize, TransportError>;
    async fn write_all(&mut self, data: &[u8]) -> Result<(), TransportError>;
    async fn close(&mut self) -> Result<(), TransportError>;
}
```

实现：

- `TcpTransport`：主动连接远端 TCP。
- `ConnectedTcpTransport`：包装已 accept 的 TCP socket。
- `SerialTransport`：打开串口。
- `ConnectedSerialTransport`：包装已有串口对象。
- `transport/can.rs`：TSMaster CAN / CAN FD 传输封装。

串口访问使用 `spawn_blocking` 包装同步 `serialport` API，避免阻塞 Tokio runtime。

### 4.6 `src/session`

会话层负责连接生命周期、命令调用、pending 请求、心跳、重连、遥测帧分发和 ACK 回包。

关键类型：

- `DeviceSession`
- `DeviceSessionHandle`
- `SessionConfig`
- `SessionError`

默认会话配置：

```text
heartbeat_interval = 2s
heartbeat_timeout  = 8s
reconnect_base     = 200ms
reconnect_max      = 5s
enable_heartbeat   = true
reconnect_enabled  = true
```

TCP ingress 接收到的被动连接会创建 `DeviceSession`，但使用：

```text
enable_heartbeat = false
reconnect_enabled = false
```

原因是这类连接来自设备端主动连入。连接断开后，由设备端重新连接即可。

会话层收到 `kind = 0x34` 且 payload 可解析为 `sid/value` 时：

1. 更新 `Store` 中设备快照。
2. 发布 `DeviceEvent::TelemetrySample`。
3. 回发 `kind = 0x90` 的 ACK，payload 为 `demo2-ack`。

### 4.7 `src/ingress`

接入层把外部输入转为 `AppEvent`。

#### TCP ingress

文件：`src/ingress/tcp.rs`

`run_tcp_ingress` 会绑定配置中的 `ingress_addr`，默认 `127.0.0.1:19010`。每个 TCP 连接会被包装为 `ConnectedTcpTransport`，并创建一个 `DeviceSession`。

设备 ID 格式：

```text
tcp://<peer_addr>
```

#### 串口 ingress

文件：`src/ingress/serial.rs`

串口模式：

- `legacy`
- `demo`
- `sent`
- `sent1`
- `sent2`
- `sent3`

`sent1` / `sent2` / `sent3` 在 collector 配置解析时都会归为 `SerialIngressMode::Sent`，具体信号由 SENT 帧 `pause` 字段区分。

设备 ID 格式：

```text
serial://<port_name>
```

#### CAN ingress

文件：`src/ingress/can.rs`

CAN 入口会：

- 尝试连接 TSMaster / TC1012。
- 支持按硬件名和通道做候选探测。
- 读取 CAN / CAN FD 回调帧。
- 解码普通三轴 CAN 样本。
- 解码 SENT over CAN 样本。
- 解码 SENT error 帧并转为告警事件。
- 支持 CAN TX 队列，供 UI 发起自检帧。

CAN 数据解析约定：

- `identifier = 0x100` -> sensor 0 / axis x
- `identifier = 0x102` -> sensor 1 / axis y
- `identifier = 0x104` -> sensor 2 / axis z
- `identifier = 3` -> SENT error
- 其他非 TX 且数据长度足够的 CAN FD 帧可解析 SENT values：
  - sensor 0：T1 angle，读取 offset 25 的 little-endian f32
  - sensor 1：T1 torque，读取 offset 29 的 little-endian f32
  - sensor 2：T2 angle，读取 offset 1 的 little-endian f32
  - sensor 3：T2 torque，读取 offset 5 的 little-endian f32
  - sensor 4：S angle，读取 offset 49 的 little-endian f32

SENT values 默认经过移动平均滤波。角度信号使用圆周平均，扭矩信号使用普通算术平均。

### 4.8 `src/signal`

信号处理模块把原始 sensor 样本映射为可展示的信号规格，并支持派生信号。

默认信号：

- sensor 0：`sent_t1_angle`，单位 `deg`
- sensor 1：`sent_t1_torque`，单位 `Nm`
- sensor 2：`sent_t2_angle`，单位 `deg`
- sensor 3：`sent_t2_torque`，单位 `Nm`
- sensor 4：`sent_s_angle`，单位 `deg`

额外派生角度：

- `sensor_0_angle`
- `sensor_1_angle`
- `sensor_2_angle`
- `sensor_3_angle`

派生公式支持：

- `ScaleOffset`：`output = input * scale + offset`
- `OffsetScale`：`output = (input - input_offset) * scale`

### 4.9 `src/db`

数据库 schema 定义在 `SCHEMA_SQL` 中。

主要表：

- `telemetry_samples`
- `alarm_events`
- `system_events`

`telemetry_samples` 是按 `created_at` 范围分区的表。schema 会创建函数：

```sql
ensure_telemetry_partition_for_day(target_day DATE)
```

采集服务批量写入遥测前，会根据样本时间确保对应日期分区存在。

## 5. 二进制入口

### 5.1 `collector_service`

核心采集服务。职责：

- 加载 `config.toml`。
- 创建 raw event bus 和 processed event bus。
- 启动可选数据库写入器。
- 启动可选 CAN ingress。
- 启动可选串口 ingress。
- 启动 TCP ingress。
- 启动 UI feed server。
- 启动健康检查 server。
- 统计接入状态、样本数、丢弃数和数据库错误。

默认端口：

```text
TCP ingress: 127.0.0.1:19010
UI feed:     127.0.0.1:19011
Health:      127.0.0.1:19012
```

### 5.2 `ui_client`

桌面 UI 客户端，基于 eframe / egui。

职责：

- 启动内嵌 collector runtime。
- 连接 UI feed 地址。
- 接收 JSON 行流。
- 展示实时曲线。
- 展示 demo / SENT / CAN / TCP 视图。
- 展示告警面板和历史告警。
- 支持 CAN 自检发送。
- 支持 CAN 历史数据回放和导出。
- 从 PostgreSQL 查询告警记录和历史样本。

UI feed 地址来源优先级：

1. 环境变量 `DEMO2_UI_FEED_ADDR`
2. `config.toml` 中 `[collector].ui_feed_addr`
3. 默认 `127.0.0.1:19011`

PostgreSQL DSN 来源优先级：

1. 环境变量 `DEMO2_PG_DSN`
2. `config.toml` 中 `[collector].pg_dsn`
3. 默认 `host=127.0.0.1 port=5432 user=postgres password=123456 dbname=demo2`

### 5.3 `init_db`

数据库初始化工具。职责：

- 连接 bootstrap 数据库，默认 `postgres`。
- 若目标数据库不存在则创建。
- 连接目标数据库并执行 `SCHEMA_SQL`。

常用命令：

```powershell
cargo run --bin init_db
```

支持参数：

```text
--host <host>
--port <port>
--user <user>
--password <password>
--database <name>
--bootstrap-db <name>
```

支持环境变量：

```text
DEMO2_DB_HOST
DEMO2_DB_PORT
DEMO2_DB_USER
DEMO2_DB_PASSWORD
DEMO2_DB_NAME
DEMO2_BOOTSTRAP_DB
```

### 5.4 `check_db_status`

数据库状态检查工具，会读取 `DEMO2_DB_CHECK_DSN`，否则使用默认 DSN。用于查看表、分区和最近遥测记录。

```powershell
cargo run --bin check_db_status
```

### 5.5 `check_db_partition`

数据库分区检查工具，会读取 `DEMO2_DB_CHECK_DSN`，否则使用默认 DSN。

```powershell
cargo run --bin check_db_partition
```

### 5.6 `sender_stress_report`

TCP 压测发送端。职责：

- 连接 collector TCP ingress。
- 发送 `kind = 0x34` 的 legacy 遥测帧。
- 读取 ACK。
- 分阶段提升发送压力并统计结果。

目标地址：

1. 环境变量 `DEMO2_INGRESS_ADDR`
2. 默认 `127.0.0.1:19010`

启动：

```powershell
cargo run --bin sender_stress_report
```

### 5.7 `sender_1min`

TCP 回放发送端。职责：

- 读取导出的文本数据。
- 回放前一段时间窗口的数据。
- 转换为 `sid/value` legacy payload 发送给 collector。

配置：

- `DEMO2_INGRESS_ADDR`
- `DEMO2_EXPORT_PATH`

启动：

```powershell
$env:DEMO2_EXPORT_PATH = "D:\path\to\export.txt"
cargo run --bin sender_1min
```

### 5.8 `serial_frame_sender`

串口测试发送端，支持 legacy 和 SENT 帧。

启动示例：

```powershell
cargo run --bin serial_frame_sender -- --port COM4 --baud 2000000 --format sent1
```

参数：

```text
--port <COMx>           默认 COM4
--baud <n>              默认 2000000
--format <mode>         legacy | sent1 | sent2 | sent3，默认 sent1
--sensors <n>           虚拟 pair 数，默认 4
--interval-ms <n>       legacy 发送间隔，默认 30
--duration-secs <n>     运行时长，默认 60
```

环境变量：

```text
DEMO2_SERIAL_PORT
DEMO2_SERIAL_BAUD
DEMO2_SERIAL_SENSORS
DEMO2_SERIAL_INTERVAL_MS
DEMO2_SERIAL_DURATION_SECS
DEMO2_SERIAL_FORMAT
```

### 5.9 `serial_sender_ui`

串口发送端 UI，基于 eframe / egui。它提供图形化方式配置串口、输出格式和发送行为，内部支持 legacy、demo 和 SENT 发送逻辑。

```powershell
cargo run --bin serial_sender_ui
```

## 6. 配置说明

采集服务读取当前工作目录下的 `config.toml`。如果文件不存在或解析失败，则回退到 `CollectorConfig::default()`。

示例配置位于 `config.toml.example`。注意：当前 `docker-compose.yml` 中 PostgreSQL 密码是 `123456`，而 `config.toml.example` 中 `pg_dsn` 密码为 `postgres`。如果使用仓库自带 Docker Compose，建议把 `pg_dsn` 密码改为 `123456`，或同步修改 Docker Compose 的密码。

常用配置项：

```toml
[collector]
ingress_addr = "127.0.0.1:19010"
ui_feed_addr = "127.0.0.1:19011"
health_addr = "127.0.0.1:19012"

pg_dsn = "host=127.0.0.1 port=5432 user=postgres password=123456 dbname=demo2"
pg_connect_max_retries = 20
pg_connect_retry_ms = 1000

serial_port = "COM3"
serial_baud = 2000000
serial_mode = "sent"

can_enabled = false
can_hardware_name = "TC1012"
can_channel = 0
can_baud_kbps = 500
can_data_baud_kbps = 2000
can_autostart_tsmaster = true

db_filter_enabled = false
db_filter_order = 10
db_filter_sample_rate_hz = 48000.0
db_filter_cutoff_hz = 4000.0

sent_filter_enabled = true
sent_filter_window = 10

max_payload = 4096
bus_capacity = 10000
ui_feed_capacity = 10000
```

当前本地 `config.toml` 中有这些值得注意的设置：

- `ingress_addr = "0.0.0.0:19010"`，允许外部主机连接 TCP ingress。
- `can_enabled = true`，启动时会尝试打开 CAN。
- `serial_port = "COM3"` 且 `serial_mode = "demo"`，启动时会尝试打开 COM3。

如果只是跑 TCP sender 联调，可以临时关闭 CAN 和串口：

```toml
can_enabled = false
# 删除 serial_port，或注释掉 serial_port
```

或者通过环境变量覆盖。

## 7. 环境变量汇总

### collector

```text
DEMO2_DISABLE_DB
DEMO2_COLLECTOR_SERIAL_PORT
DEMO2_COLLECTOR_SERIAL_BAUD
DEMO2_COLLECTOR_SERIAL_MODE
DEMO2_COLLECTOR_CAN_ENABLED
DEMO2_COLLECTOR_CAN_CHANNEL
DEMO2_COLLECTOR_CAN_HW_NAME
DEMO2_COLLECTOR_CAN_TSMASTER_BIN
DEMO2_COLLECTOR_CAN_AUTOSTART_TSMASTER
DEMO2_COLLECTOR_CAN_BAUD_KBPS
DEMO2_COLLECTOR_CAN_DATA_BAUD_KBPS
DEMO2_SENT_FILTER_ENABLED
DEMO2_SENT_FILTER_WINDOW
```

布尔值支持：

```text
1 / true / yes / on
0 / false / no / off
```

### UI

```text
DEMO2_UI_FEED_ADDR
DEMO2_PG_DSN
```

### 数据库工具

```text
DEMO2_DB_HOST
DEMO2_DB_PORT
DEMO2_DB_USER
DEMO2_DB_PASSWORD
DEMO2_DB_NAME
DEMO2_BOOTSTRAP_DB
DEMO2_DB_CHECK_DSN
```

### sender

```text
DEMO2_INGRESS_ADDR
DEMO2_EXPORT_PATH
DEMO2_SERIAL_PORT
DEMO2_SERIAL_BAUD
DEMO2_SERIAL_SENSORS
DEMO2_SERIAL_INTERVAL_MS
DEMO2_SERIAL_DURATION_SECS
DEMO2_SERIAL_FORMAT
```

### CAN / TSMaster

```text
TSMASTER_BIN
```

如果未配置 TSMaster 路径，代码默认查找：

```text
D:\TSMaster\bin64
```

## 8. 运行步骤

### 8.1 准备数据库

启动 PostgreSQL：

```powershell
docker compose up -d postgres
```

初始化数据库：

```powershell
cargo run --bin init_db
```

如果不需要数据库持久化：

```powershell
$env:DEMO2_DISABLE_DB = "1"
```

### 8.2 启动采集服务

```powershell
cargo run --bin collector_service
```

启动成功后会打印：

```text
collector ingress listening on ...
collector ui feed listening on ...
collector health listening on ...
```

### 8.3 启动 UI

```powershell
cargo run --bin ui_client
```

注意：当前 `ui_client` 会内嵌启动 collector。如果已经有独立 `collector_service` 占用了同一组端口，UI 内嵌 collector 可能因为端口占用退出，但 UI feed 线程仍会尝试连接配置中的 `ui_feed_addr`。

### 8.4 发送 TCP 测试数据

```powershell
cargo run --bin sender_stress_report
```

或回放导出文件：

```powershell
$env:DEMO2_EXPORT_PATH = "D:\path\to\export.txt"
cargo run --bin sender_1min
```

### 8.5 串口联调

采集端配置：

```toml
[collector]
serial_port = "COM3"
serial_baud = 2000000
serial_mode = "sent"
```

发送端：

```powershell
cargo run --bin serial_frame_sender -- --port COM4 --baud 2000000 --format sent1
```

### 8.6 CAN 联调

配置示例：

```toml
[collector]
can_enabled = true
can_hardware_name = "TC1012"
can_channel = 0
can_baud_kbps = 500
can_data_baud_kbps = 2000
can_autostart_tsmaster = true
```

如果 TSMaster 不在默认路径：

```powershell
$env:DEMO2_COLLECTOR_CAN_TSMASTER_BIN = "D:\TSMaster\bin64"
```

或：

```powershell
$env:TSMASTER_BIN = "D:\TSMaster\bin64"
```

## 9. 一键启动脚本

脚本：

```powershell
.\scripts\run_demo2.ps1
```

参数：

```powershell
.\scripts\run_demo2.ps1 -WithDocker
.\scripts\run_demo2.ps1 -WithDocker -WithSender
.\scripts\run_demo2.ps1 -WithSender -SenderBin sender_stress_report
```

脚本行为：

- 如缺少 `config.toml`，从 `config.toml.example` 自动复制。
- `-WithDocker` 时启动 `postgres` 容器。
- 分别为 collector、UI、sender 设置独立 `CARGO_TARGET_DIR`。
- 通过新 PowerShell 窗口启动各进程。

当前脚本中的中文提示存在编码损坏，但逻辑仍可读。

## 10. 数据流

### 10.1 TCP sender 到 UI

```text
sender_stress_report / sender_1min
  -> TCP 127.0.0.1:19010
  -> run_tcp_ingress
  -> DeviceSession
  -> SimpleFrameCodec
  -> AppEvent::Device(TelemetrySample)
  -> raw_bus
  -> run_filtered_event_forwarder
  -> processed_bus
  -> run_ui_forwarder
  -> JSON line on 127.0.0.1:19011
  -> ui_client feed_thread
  -> egui 曲线窗口
```

### 10.2 串口 demo 到 UI

```text
SerialTransport
  -> SerialDemoCodec
  -> StreamFrame groups
  -> ch1 x/y/z -> sensor 0/1/2
  -> alarm bit -> AlarmRaised / AlarmCleared
  -> EventBus
  -> UI feed / DB
```

### 10.3 CAN SENT 到 UI

```text
TSMaster callback
  -> CanTransport
  -> run_can_ingress
  -> decode_sent_values
  -> optional SentMovingAverage
  -> TelemetrySample source_kind=CanSent
  -> EventBus
  -> UI feed / DB
```

### 10.4 持久化链路

```text
processed_bus
  -> run_persistence_forwarder
  -> DbCmd channel
  -> PostgreSQL writer
  -> telemetry_samples / alarm_events / system_events
```

遥测写入使用批量 `UNNEST`，批大小为 256。数据库命令通道容量为 50000。

## 11. UI feed 消息格式

采集服务向 UI feed 输出 JSON 行，每行一个消息。枚举使用 serde tag：

```json
{"type":"telemetry","payload":{...}}
{"type":"alarm","payload":{...}}
{"type":"status","payload":"..."}
```

Telemetry payload：

```json
{
  "device_id": "tcp://127.0.0.1:54321",
  "sensor_id": 0,
  "axis": "",
  "alarm_bit": false,
  "t_sec": 1.23,
  "value": 47.381,
  "request_id": 100,
  "source_kind": "TcpFrame"
}
```

`axis` 由 collector 根据来源和 sensor_id 填充：

- `SerialDemo`：0/1/2 -> `x` / `y` / `z`
- `CanAxis`：0/1/2 -> `x` / `y` / `z`
- `CanSent`：0/1/2/3/4 -> `t1_angle` / `t1_torque` / `t2_angle` / `t2_torque` / `s_angle`
- 其他来源为空字符串

## 12. 数据库设计

### 12.1 `telemetry_samples`

字段：

```text
id BIGINT GENERATED ALWAYS AS IDENTITY
ts_ms BIGINT NOT NULL
created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
device_id TEXT NOT NULL
sensor_id INTEGER NOT NULL
axis TEXT NOT NULL DEFAULT ''
alarm_bit BOOLEAN NOT NULL DEFAULT FALSE
t_sec DOUBLE PRECISION NOT NULL
value DOUBLE PRECISION NOT NULL
request_id BIGINT NOT NULL
PRIMARY KEY (created_at, id)
```

分区：

```text
PARTITION BY RANGE (created_at)
```

每个日期分区会创建索引：

- `ts_ms`
- `(device_id, sensor_id, ts_ms)`
- `(device_id, axis, ts_ms)`

### 12.2 `alarm_events`

字段：

```text
id BIGSERIAL PRIMARY KEY
ts_ms BIGINT NOT NULL
created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
device_id TEXT NOT NULL
alarm_id TEXT NOT NULL
level TEXT NOT NULL
message TEXT NOT NULL
cleared BOOLEAN NOT NULL
```

索引：

- `ts_ms`
- `created_at`
- `(device_id, alarm_id, ts_ms)`

### 12.3 `system_events`

字段：

```text
id BIGSERIAL PRIMARY KEY
ts_ms BIGINT NOT NULL
created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
level TEXT NOT NULL
message TEXT NOT NULL
```

索引：

- `ts_ms`
- `created_at`

## 13. 健康检查

健康检查地址默认：

```text
http://127.0.0.1:19012
```

接口：

```powershell
Invoke-RestMethod http://127.0.0.1:19012/health
Invoke-RestMethod http://127.0.0.1:19012/ready
```

`/health` 返回：

```json
{
  "ingress_ready": true,
  "ui_ready": true,
  "ingress_connections": 1,
  "ui_clients": 1,
  "samples_rx": 12345,
  "ui_drop": 0,
  "db_drop": 0,
  "db_write_fail": 0,
  "last_db_error": null
}
```

`/ready` 在 ingress 和 UI feed 均 ready 时返回 HTTP 200，否则返回 503。

## 14. 告警机制

当前仓库中存在两套告警相关逻辑：

1. `src/app/alarm_service.rs` 定义了 `AlarmRule` 和 `AlarmService`，但 `evaluate_sample` 当前为空实现。因此 collector 的通用阈值告警服务目前不会产生新告警。
2. 接入层和 UI 中存在专项告警：
   - 串口 demo alarm bit 会发布 `demo_alarm_bit` 告警。
   - CAN SENT error 会发布 `sent_error_<type>` 告警。
   - UI 内部有 CAN 阈值、SENT 跳变等展示和记录逻辑。

如果后续要恢复通用阈值告警，应优先补全 `AlarmService::evaluate_sample`，并明确规则来源、状态存储和恢复条件。

## 15. 滤波机制

### 15.1 DB 写入前 Butterworth 低通滤波

配置：

```toml
db_filter_enabled = false
db_filter_order = 10
db_filter_sample_rate_hz = 48000.0
db_filter_cutoff_hz = 4000.0
```

该滤波器运行在 raw event bus 到 processed event bus 的转发阶段。启用后，UI feed 和 DB 都会看到处理后的值。

要求：

- order 至少为 2。
- order 必须为偶数。
- sample rate 必须为正数。
- cutoff 必须大于 0 且小于 Nyquist 频率。

### 15.2 CAN SENT 移动平均滤波

配置：

```toml
sent_filter_enabled = true
sent_filter_window = 10
```

角度信号使用圆周平均，避免 359 度与 0 度附近被错误平均到 180 度。扭矩信号使用普通算术平均。

## 16. 常见问题排查

### 16.1 端口被占用

现象：

- collector 启动失败。
- UI 内嵌 collector 报错。
- sender 出现 `os error 10061`。

排查：

```powershell
netstat -ano | findstr 19010
netstat -ano | findstr 19011
netstat -ano | findstr 19012
```

处理：

- 如果使用独立 collector + UI，考虑移除 UI 内嵌 collector 逻辑或让 UI 只连外部 feed。
- 或者调整 `config.toml` 中端口。

### 16.2 UI 没有数据

检查顺序：

1. `collector_service` 是否启动。
2. `/health` 中 `samples_rx` 是否增长。
3. `/health` 中 `ui_clients` 是否大于 0。
4. sender 是否连接到 `ingress_addr`。
5. UI 是否连接到 `ui_feed_addr`。
6. `source_kind` 是否被 UI 视图过滤到了当前窗口之外。

### 16.3 数据库无数据

检查：

1. PostgreSQL 容器是否启动。
2. `pg_dsn` 密码是否与 Docker Compose 一致。
3. 是否设置了 `DEMO2_DISABLE_DB=1`。
4. `/health` 中 `db_write_fail` 和 `last_db_error`。
5. 分区是否创建成功：

```powershell
cargo run --bin check_db_partition
```

### 16.4 串口打不开

检查：

1. 端口号是否正确，例如 `COM3` / `COM4`。
2. 是否被其他程序占用。
3. 波特率是否一致。
4. `serial_mode` 是否与发送端格式一致。

### 16.5 CAN 打不开

检查：

1. TSMaster 是否安装。
2. `TSMaster.dll` 是否在 `D:\TSMaster\bin64` 或配置的路径下。
3. 硬件名是否为 `TC1012`。
4. 通道号是否正确。
5. 是否需要关闭自动启动：

```powershell
$env:DEMO2_COLLECTOR_CAN_AUTOSTART_TSMASTER = "0"
```

### 16.6 中文显示乱码

已观察到：

- `docs/architecture.md` 中文编码损坏。
- `scripts/run_demo2.ps1` 中文提示字符串损坏。
- `ui_client.rs` 中部分中文 UI 文案也已损坏。

这类问题通常不是业务逻辑错误，而是文件编码或历史保存方式导致。建议统一转为 UTF-8，并逐步修复 UI 文案和脚本提示。

## 17. 测试

运行全部测试：

```powershell
cargo test
```

仅检查编译：

```powershell
cargo check
```

当前源码中已有测试覆盖：

- SimpleFrame 粘包拆包。
- SENT 帧 roundtrip。
- SENT3 校验字段。
- Serial demo stream 解码。
- CAN SENT 角度圆周平均。
- CAN SENT 扭矩算术平均。
- Butterworth 低通滤波效果。

## 18. 扩展建议

### 18.1 新增一种 TCP frame kind

建议步骤：

1. 在 `protocol` 或文档中约定新的 kind 和 payload。
2. 在 `session::connection_loop` 中增加 kind 分支。
3. 将解码后的业务数据转换为 `DeviceEvent`。
4. 根据需要在 `collector_service` 的 UI / DB forwarder 中处理该事件。
5. 增加协议单测和端到端联调 sender。

### 18.2 新增一种串口协议

建议步骤：

1. 在 `protocol/` 下新增 codec。
2. 在 `SerialIngressMode` 中新增模式。
3. 扩展 `parse_serial_mode`。
4. 在 `run_serial_ingress` 中增加解码分支。
5. 明确 `TelemetrySourceKind`，保证 UI 能正确分组展示。

### 18.3 完善通用告警服务

建议步骤：

1. 为 `AlarmService` 增加按 `(device_id, sensor_id)` 维护的状态。
2. 实现高高/高/低/低低或现有 high/low 规则。
3. 使用 hysteresis，即触发阈值与恢复阈值分开。
4. 发布 `AlarmRaised` 和 `AlarmCleared`。
5. 增加单元测试覆盖反复抖动场景。

### 18.4 拆分 UI 内嵌 collector

当前 `ui_client` 同时内嵌 collector，这对单进程演示方便，但对部署和联调容易造成端口冲突。建议后续明确两种入口：

- `ui_client`：只作为外部 collector 的客户端。
- `ui_client_embedded`：单进程演示版。

这样可以让生产/联调路径更清晰。

### 18.5 修复编码损坏文件

建议优先级：

1. `ui_client.rs` 中的可见 UI 文案。
2. `scripts/run_demo2.ps1` 中的中文提示。
3. 旧 `docs/architecture.md`。

修复后统一保存为 UTF-8。

## 19. 推荐开发流程

1. 修改源码前先确认当前运行模式：TCP、串口、CAN 还是 UI。
2. 小改动先运行：

```powershell
cargo check
```

3. 协议、滤波、告警或数据库改动后运行：

```powershell
cargo test
```

4. 涉及 collector 的改动，至少做一次：

```powershell
cargo run --bin collector_service
Invoke-RestMethod http://127.0.0.1:19012/health
```

5. 涉及 UI feed 的改动，用 sender 验证完整链路：

```powershell
cargo run --bin sender_stress_report
```

6. 涉及 PostgreSQL 的改动，检查：

```powershell
cargo run --bin check_db_status
cargo run --bin check_db_partition
```

## 20. 当前成熟度判断

当前项目适合：

- 本地开发联调。
- 设备协议 PoC。
- UI 展示演示。
- 小规模采集验证。

进入更稳定的生产形态前，建议补齐：

- UI 和 collector 运行模式拆分。
- 通用告警服务实现。
- 文件编码统一修复。
- 配置默认值和 Docker Compose 密码一致性。
- 更完整的集成测试。
- 指标系统和结构化日志采集。
- CAN / 串口错误恢复策略。
- 数据库写入失败后的重试、缓冲或补偿机制。

## 21. 快速命令清单

```powershell
# 启动 PostgreSQL
docker compose up -d postgres

# 初始化数据库
cargo run --bin init_db

# 启动采集服务
cargo run --bin collector_service

# 启动 UI
cargo run --bin ui_client

# TCP 压测发送
cargo run --bin sender_stress_report

# 串口 SENT 发送
cargo run --bin serial_frame_sender -- --port COM4 --baud 2000000 --format sent1

# 健康检查
Invoke-RestMethod http://127.0.0.1:19012/health
Invoke-RestMethod http://127.0.0.1:19012/ready

# 数据库检查
cargo run --bin check_db_status
cargo run --bin check_db_partition

# 测试
cargo test
cargo check
```
