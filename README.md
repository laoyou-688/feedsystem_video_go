# Feed System Video Go

基于 Go 的短视频 Feed 系统，包含账号、视频、点赞、评论、关注与 Feed 流等核心能力，支持 Redis 缓存、RabbitMQ 异步 Worker，以及基于 Docker Compose 的一键依赖启动。

## 项目特点

- 支持最新流、关注流、热度流等多种 Feed 获取方式
- 关注流采用基于粉丝规模阈值与活跃窗口的混合分发策略
- 点赞、评论链路通过 RabbitMQ 异步削峰，Worker 侧异步落库与热度更新
- 使用本地缓存 + Redis 两级缓存优化热点详情读取
- 提供 Linux 与 Windows 可运行的压测脚本，可对本地缓存、Redis 与 MySQL 回源路径进行对比
- 新增 Prometheus 指标暴露与结构化访问日志，便于压测取证和性能报告沉淀

## 技术栈

- 后端：Go、Gin、GORM、MySQL
- 缓存：Redis、本地缓存
- 消息队列：RabbitMQ
- 可观测性：Prometheus 指标、pprof、访问日志
- 部署：Docker、Docker Compose
- 压测：wrk、Lua、PowerShell

## 目录结构

```text
backend/     后端 API、Worker、核心业务逻辑
frontend/    前端页面与接口调用
scripts/     压测与辅助脚本
docs/        性能报告模板与后续文档沉淀
```

## 使用 Docker Compose 启动

要求：
- 已安装 Docker
- 已安装 Docker Compose

在项目根目录执行：

```bash
docker compose up -d --build
```

启动后默认访问地址：
- 后端 API：`http://127.0.0.1:8080`
- 前端页面：`http://127.0.0.1:5173`
- RabbitMQ 管理台：`http://127.0.0.1:15672`
- Prometheus 指标：`http://127.0.0.1:8080/metrics`
- API pprof：按 `backend/configs/config.yaml` 配置

## 本地开发启动

先启动依赖：

```bash
docker compose up -d mysql redis rabbitmq
```

启动后端 API：

```bash
cd backend
go run ./cmd
```

启动 Worker：

```bash
cd backend
go run ./cmd/worker
```

启动前端：

```bash
cd frontend
npm install
npm run dev
```

## 可观测性

当前版本已经内置两类观测能力：

1. `pprof`

- API：`backend/configs/config.yaml` 中 `observability.pprof.api_addr`
- Worker：`backend/configs/config.yaml` 中 `observability.pprof.worker_addr`

2. Prometheus 指标

- 默认路径：`/metrics`
- 配置项：`observability.metrics.enabled`、`observability.metrics.path`

当前可直接使用的指标包括：

- `feedsystem_http_requests_total`
- `feedsystem_http_request_duration_seconds`
- `feedsystem_http_in_flight_requests`

访问日志会按结构化字段输出，适合压测期间结合接口延迟一起分析。

## 压测脚本

仓库内提供了视频详情接口和 Feed 列表接口的对比压测脚本。

### 视频详情压测

Linux / macOS：

```bash
HOST=http://127.0.0.1:8080 bash scripts/perf/prepare_perf_data.sh
VIDEO_ID=<你的video_id> HOST=http://127.0.0.1:8080 bash scripts/perf/run_video_detail_perf.sh
```

Windows PowerShell：

```powershell
$env:HOST = "http://127.0.0.1:8080"
.\scripts\perf\prepare_perf_data.ps1

$env:HOST = "http://127.0.0.1:8080"
$env:VIDEO_ID = "<你的video_id>"
.\scripts\perf\run_video_detail_perf.ps1
```

可选参数：

```powershell
$env:CONNECTIONS = "100"
$env:THREADS = "8"
$env:DURATION = "20s"
$env:WRK_BIN = "wrk.exe"
```

### Feed 列表压测

相比单条视频详情，`/feed/listLatest` 更容易体现时间线读取与视频实体缓存的差异。

1. 准备 Feed 压测数据

```bash
HOST=http://127.0.0.1:8080 VIDEO_COUNT=30 bash scripts/perf/prepare_feed_perf_data.sh
```

2. 运行 Feed 压测

```bash
HOST=http://127.0.0.1:8080 bash scripts/perf/run_feed_list_latest_perf.sh
```

这个脚本会依次压测三种模式：

- `timeline + auto entity cache`
- `timeline + mysql entity`
- `mysql + mysql`

含义分别是：

- 第一组：时间线按默认策略读取，视频实体也按默认缓存策略读取
- 第二组：时间线仍从时间线链路读取，但视频实体强制 MySQL 回源
- 第三组：时间线与视频实体都强制走 MySQL

这组数据通常比 `video/getDetail` 更容易拉开差距。

## 性能报告模板

建议把每次压测结果按同一模板记录，方便后续写进简历或面试回答。

- 模板文件：[docs/performance-report-template.md](docs/performance-report-template.md)

建议至少记录：

- 本地缓存 vs Redis vs MySQL 的详情接口对比
- `/feed/listLatest` 或 `/feed/listByPopularity` 的读路径表现
- RabbitMQ 正常与降级直写时的写链路差异
- 关键慢 SQL 与 Redis 命中情况

## 下一步建议

在当前版本基础上，最值得继续补的方向是：

1. 给 Feed 与 Like 链路增加更细粒度的业务指标
2. 把 Prometheus / Grafana 接入 `docker-compose`
3. 补 Like 写链路的压测脚本和降级测试
4. 为热点 SQL 增加索引与 EXPLAIN 对比文档
