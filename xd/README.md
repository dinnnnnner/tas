# demo2 V2 (MVP)

面向上位机 V2 的最小可用实现，核心目标是从“围绕 socket 编程”切到“围绕设备会话 + 业务命令编程”。

## 详细文档

- 架构与实现全量说明：`docs/architecture.md`

## 架构分层

- `transport`: 字节流收发（`Transport trait`, `TcpTransport`）
- `protocol`: 帧编解码与流式解包（`FrameCodec`, `SimpleFrameCodec`）
- `session`: 设备会话状态机、心跳、超时、重连、pending 请求表（`DeviceSession`）
- `app`: 多设备编排与业务服务（`DeviceManager`, `CommandService`, `AlarmService`）
- `bus`: 事件总线与状态缓存（`EventBus`, `Store`）

## 当前运行模式（推荐）

已拆分为“采集服务”和“UI客户端”两个进程：

- `collector_service`（无界面）
  - 设备接入监听：`127.0.0.1:19010`
  - UI 数据流输出：`127.0.0.1:19011`（JSON 行流）
  - 健康检查：`127.0.0.1:19012`（`/health`, `/ready`）
  - 可选串口接入：`serial_port + serial_baud + serial_mode`
  - 持久化：PostgreSQL
- `ui_client`（有界面）
  - 连接 `collector_service` 的 `19011` 端口订阅数据
  - 串口数据路径统一为 `serial -> collector_service -> ui_client`
  - 10 传感器窗口、拖拽、Ctrl+滚轮缩放、最近60秒曲线

## PostgreSQL 配置

1. 一键启动 PostgreSQL：

```bash
docker compose up -d postgres
```

2. 复制配置模板：

```bash
copy config.toml.example config.toml
```

3. 修改 `config.toml` 中 `collector` 配置，例如：

```toml
[collector]
serial_port = "COM3"
serial_baud = 2000000
serial_mode = "sent"
pg_dsn = "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=demo2"
pg_connect_max_retries = 20
pg_connect_retry_ms = 1000
```

4. 首次启动会自动建表和索引：
- 表：`telemetry_samples`, `alarm_events`, `system_events`
- 索引：按 `ts_ms`，以及 `(device_id, sensor_id, ts_ms)` 组合索引

## 二进制说明

- `cargo run --bin collector_service`: 采集与协议处理服务
- `cargo run --bin ui_client`: 分离式 UI 客户端
- `cargo run --bin acceptor`: 简单接收端（打印+ACK，调试用）
- `cargo run --bin acceptor_ui`: 单进程接收+UI（兼容保留）
- `cargo run --bin serial_frame_sender -- --port COM4 --baud 2000000 --format sent1`: 串口发送端（适合 `com0com` 联调）
- `cargo run --bin serial_sender_ui`: 串口发送 UI，可配置串口并点击开始/停止发送

## 启动顺序（联调）

1. 启动采集服务（本项目根目录）：

```bash
cargo run --bin collector_service
```

2. 启动 UI 客户端（本项目根目录）：

```bash
cargo run --bin ui_client
```

3. 启动串口发送端：

```bash
cargo run --bin serial_frame_sender -- --port COM4 --baud 2000000 --format sent1
```

或使用发送 UI：

```bash
cargo run --bin serial_sender_ui
```

## 一键启动（Windows）

在项目根目录执行：

```powershell
.\scripts\run_demo2.ps1
```

可选参数：

```powershell
.\scripts\run_demo2.ps1 -WithDocker
.\scripts\run_demo2.ps1 -WithDocker -WithSender
```

- `-WithDocker`: 自动执行 `docker compose up -d postgres`
- `-WithSender`: 自动在 `tcp_frame_sender` 目录启动发送端（默认 `sender_1min`）

也可双击或命令行运行：

```powershell
.\run_demo2.cmd
```

## 已实现能力

- 多连接采集与 ACK 回包
- TCP 粘包拆包（`BytesMut` + `try_decode`）
- EventBus 统一事件通路（遥测/告警/系统消息）
- app 层告警服务（阈值、触发/恢复事件）
- UI 多窗口实时曲线（每路最近60秒）
- PostgreSQL 持久化（遥测、告警、系统事件）
- PostgreSQL 启动重试与退避

## 备注

`simulator` 模块已移除，当前测试链路统一使用 `tcp_frame_sender`。
