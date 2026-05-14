# Feed System Video Go

一个基于 Go 实现的短视频 Feed 流练习项目，覆盖账号、视频、点赞、评论、关注与 Feed 分发等核心后端链路，并结合 Redis、RabbitMQ、Docker Compose 和压测脚本，补齐缓存、异步削峰与工程化启动能力。

## 我主要做了什么

- 补充并整理了 Feed 相关链路，包括最新流、关注流与热度流
- 在关注流中加入基于粉丝规模阈值与活跃窗口的混合分发策略
- 将点赞、评论链路接入 RabbitMQ，支持异步落库与热度更新
- 增加本地缓存、Redis 缓存、缓存失效广播与压测脚本，方便验证优化效果

## 核心能力

- 账号系统：注册、登录、改密、改名、登出
- 视频系统：发布视频、作者视频列表、视频详情
- 互动系统：点赞、取消点赞、评论发布、评论删除
- 关注系统：关注、取关、粉丝列表、关注列表
- Feed 系统：最新流、关注流、热度流

## 技术栈

- 后端：Go、Gin、GORM、MySQL
- 缓存：Redis、本地缓存
- 异步链路：RabbitMQ、Worker
- 部署方式：Docker、Docker Compose
- 压测工具：wrk、Lua

## 目录说明

```text
backend/     后端 API、Worker 与核心业务逻辑
frontend/    前端页面与联调 UI
scripts/     压测与辅助脚本
test/        接口调试相关文件
picture/     表结构图与流程图
```

## 使用 Docker Compose 启动

如果你的环境已经安装好 Docker 和 Docker Compose，可以直接在项目根目录执行：

```bash
docker compose up -d --build
```

默认访问地址：

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

仓库中提供了 Linux 下可直接执行的 `wrk + Lua` 压测脚本，当前主要用于验证视频详情接口在不同缓存路径下的性能差异。

### 1. 生成压测数据

```bash
HOST=http://127.0.0.1:8080 bash scripts/perf/prepare_perf_data.sh
```

该脚本会自动完成：

- 注册压测用户
- 登录获取 token
- 发布一条测试视频
- 输出后续压测需要用到的 `VIDEO_ID`

### 2. 执行压测

```bash
VIDEO_ID=<你的video_id> HOST=http://127.0.0.1:8080 bash scripts/perf/run_video_detail_perf.sh
```

当前会分别压三条路径：

- 本地缓存路径
- Redis 缓存路径
- MySQL 回源路径

如果需要调整并发和时长，可以这样执行：

```bash
VIDEO_ID=<你的video_id> \
HOST=http://127.0.0.1:8080 \
CONNECTIONS=100 \
THREADS=8 \
DURATION=20s \
bash scripts/perf/run_video_detail_perf.sh
```

## 备注

- 前端目录说明见 [frontend/README.md](./frontend/README.md)
- 当前项目更适合用于 Feed 流、缓存、多级存储与异步链路相关练习
