# Feed System Video Go

基于 Go 的短视频 Feed 系统，包含账号、视频、点赞、评论、关注与 Feed 流等核心能力，支持 Redis 缓存、RabbitMQ 异步 Worker，以及基于 Docker Compose 的一键依赖启动。

## 项目特点

- 支持最新流、关注流、热度流等多种 Feed 获取方式
- 关注流采用基于粉丝规模阈值与活跃窗口的混合分发策略
- 点赞、评论链路通过 RabbitMQ 异步削峰，Worker 侧异步落库与热度更新
- 使用本地缓存 + Redis 两级缓存优化热点详情读取
- 提供 Linux 可运行的压测脚本，可对本地缓存、Redis 与 MySQL 回源链路进行对比

## 技术栈

- 后端：Go、Gin、GORM、MySQL
- 缓存：Redis、本地缓存
- 消息队列：RabbitMQ
- 部署：Docker、Docker Compose
- 压测：wrk、Lua

## 目录结构

```text
backend/     后端 API、Worker、核心业务逻辑
frontend/    前端页面与接口调用
scripts/     压测与辅助脚本
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

## 压测脚本

仓库内提供了 Linux 下可直接使用的 `wrk + Lua` 压测脚本，当前用于压测视频详情接口在不同缓存路径下的表现。

### 1. 造压测数据

在项目根目录执行：

```bash
HOST=http://127.0.0.1:8080 bash scripts/perf/prepare_perf_data.sh
```

脚本会自动：

- 注册压测用户
- 登录获取 token
- 发布一条压测视频
- 输出 `VIDEO_ID`

### 2. 运行压测

```bash
VIDEO_ID=<你的video_id> HOST=http://127.0.0.1:8080 bash scripts/perf/run_video_detail_perf.sh
```

脚本会分别压测：

- 本地缓存路径
- Redis 缓存路径
- MySQL 回源路径

如果需要调整参数，可以这样运行：

```bash
VIDEO_ID=<你的video_id> \
HOST=http://127.0.0.1:8080 \
CONNECTIONS=100 \
THREADS=8 \
DURATION=20s \
bash scripts/perf/run_video_detail_perf.sh
```

