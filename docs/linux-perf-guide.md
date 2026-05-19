# Linux Pressure Test Guide

这份文档用于在 Linux 服务器上拉取项目后，完成视频详情接口的基线压测，并结合 `/metrics` 与 `pprof` 观察结果。

## 1. 服务器准备

建议准备一台普通 Linux 云主机即可，2C4G 起步就够做第一轮基线压测。

至少安装：

```bash
sudo apt-get update
sudo apt-get install -y git curl docker.io docker-compose-plugin
```

如果机器是 CentOS / Rocky / AlmaLinux，把 `apt-get` 换成对应包管理器即可。

## 2. 安装 wrk

Ubuntu / Debian 可以直接编译安装：

```bash
sudo apt-get install -y build-essential libssl-dev zlib1g-dev
git clone https://github.com/wg/wrk.git
cd wrk
make
sudo cp wrk /usr/local/bin/
wrk -v
```

如果你不想自己编译，也可以下载现成二进制，但建议优先自己编译，兼容性更稳。

## 3. 拉取项目

```bash
git clone https://github.com/laoyou-688/feedsystem_video_go.git
cd feedsystem_video_go
```

如果你后面把仓库名改了，就把这里换成你的实际地址。

## 4. 启动服务

推荐直接用 Docker Compose：

```bash
docker compose up -d --build
```

检查容器状态：

```bash
docker compose ps
```

确认后端健康：

```bash
curl http://127.0.0.1:8080/metrics
```

如果能返回 Prometheus 文本指标，说明 API 已经正常启动。

## 5. 准备压测数据

项目根目录执行：

```bash
HOST=http://127.0.0.1:8080 bash scripts/perf/prepare_perf_data.sh
```

你会看到类似输出：

```bash
HOST=http://127.0.0.1:8080
USERNAME=perf_user_xxx
TOKEN=xxx
VIDEO_ID=123
```

把 `VIDEO_ID` 记下来。

## 6. 运行视频详情压测

```bash
VIDEO_ID=123 HOST=http://127.0.0.1:8080 bash scripts/perf/run_video_detail_perf.sh
```

这个脚本会依次压三条路径：

1. `local cache`
2. `redis cache`
3. `mysql direct`

默认参数：

- `THREADS=4`
- `CONNECTIONS=50`
- `DURATION=15s`

你可以调整成更激进一些：

```bash
VIDEO_ID=123 \
HOST=http://127.0.0.1:8080 \
THREADS=8 \
CONNECTIONS=100 \
DURATION=30s \
bash scripts/perf/run_video_detail_perf.sh
```

## 7. 压测时同步观察什么

### 7.1 看接口指标

压测前后各抓一次：

```bash
curl http://127.0.0.1:8080/metrics > metrics-before.txt
curl http://127.0.0.1:8080/metrics > metrics-after.txt
```

重点看：

- `feedsystem_http_requests_total`
- `feedsystem_http_request_duration_seconds`
- `feedsystem_http_in_flight_requests`

### 7.2 看容器资源

```bash
docker stats
```

重点看：

- backend CPU / memory
- mysql CPU
- redis CPU

### 7.3 看 pprof

如果你想在 Linux 上打开 pprof，可以把 `backend/configs/config.docker.yaml` 中的 `observability.pprof.enabled` 改成 `true`，并给 API 容器暴露端口。

然后可以抓 CPU profile：

```bash
curl http://127.0.0.1:6060/debug/pprof/profile?seconds=20 --output cpu.pprof
```

## 8. 建议怎么记录结果

把每次压测结果填写到：

- `docs/performance-report-template.md`

至少记录：

1. 本次机器配置
2. wrk 参数
3. local / redis / mysql 三组数据
4. 哪条链路瓶颈最明显
5. `/metrics` 和 `docker stats` 的关键观察

## 9. 最适合你现在写进简历的结论

你这轮压测最适合产出的不是“极限 QPS 很高”，而是：

1. 验证本地缓存、Redis 缓存与 MySQL 回源链路存在显著性能差异
2. 证明 `/metrics + pprof + wrk` 已经形成可复现的性能分析闭环
3. 为后续优化 Feed、Like、MQ 写链路提供定量基线

## 10. 下一步推荐

完成这轮视频详情压测后，建议继续做两件事：

1. 给 `/feed/listLatest` 增加压测脚本
2. 给 `/like/like` 增加 RabbitMQ 正常 / 降级直写的对比压测
