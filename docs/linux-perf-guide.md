# Linux Pressure Test Guide

这份文档用于在 Linux 服务器上拉取项目后，完成视频详情接口和 Feed 列表接口的基线压测，并结合 `/metrics` 与 `pprof` 观察结果。

## 1. 服务器准备

建议准备一台普通 Linux 云主机即可，2C4G 起步就够做第一轮基线压测。

至少安装：

```bash
sudo apt-get update
sudo apt-get install -y git curl docker.io docker-compose-plugin
```

## 2. 安装 wrk

```bash
sudo apt-get install -y build-essential libssl-dev zlib1g-dev
git clone https://github.com/wg/wrk.git
cd wrk
make
sudo cp wrk /usr/local/bin/
wrk -v
```

## 3. 拉取项目

```bash
git clone https://github.com/laoyou-688/feedsystem_video_go.git
cd feedsystem_video_go
```

## 4. 启动服务

```bash
docker compose up -d --build
docker compose ps
curl http://127.0.0.1:8080/metrics
```

## 5. 准备视频详情压测数据

```bash
HOST=http://127.0.0.1:8080 bash scripts/perf/prepare_perf_data.sh
```

你会得到 `VIDEO_ID`。

## 6. 运行视频详情压测

```bash
VIDEO_ID=123 HOST=http://127.0.0.1:8080 bash scripts/perf/run_video_detail_perf.sh
```

更高压版本：

```bash
VIDEO_ID=123 \
HOST=http://127.0.0.1:8080 \
THREADS=8 \
CONNECTIONS=100 \
DURATION=30s \
bash scripts/perf/run_video_detail_perf.sh
```

## 7. 准备 Feed 压测数据

```bash
HOST=http://127.0.0.1:8080 VIDEO_COUNT=30 bash scripts/perf/prepare_feed_perf_data.sh
```

这个脚本会生成一批新视频，让 `/feed/listLatest` 更容易形成稳定差异。

## 8. 运行 Feed 列表压测

```bash
HOST=http://127.0.0.1:8080 bash scripts/perf/run_feed_list_latest_perf.sh
```

这个脚本会自动对比三种模式：

1. `timeline + auto entity cache`
2. `timeline + mysql entity`
3. `mysql + mysql`

默认是匿名基线压测，不会带 `X-Feed-Session`，这样可以先测 Feed 拉取、时间线和实体缓存本身的性能。

如果你还想单独观察“曝光去重”带来的额外成本，再加一个固定 session：

```bash
HOST=http://127.0.0.1:8080 \
FEED_SESSION=perf-session-001 \
bash scripts/perf/run_feed_list_latest_perf.sh
```

## 9. 压测时同步观察什么

### 9.1 接口指标

```bash
curl http://127.0.0.1:8080/metrics > metrics-before.txt
curl http://127.0.0.1:8080/metrics > metrics-after.txt
```

重点看：

- `feedsystem_http_requests_total`
- `feedsystem_http_request_duration_seconds`
- `feedsystem_http_in_flight_requests`

### 9.2 容器资源

```bash
docker stats
```

### 9.3 pprof

如果需要抓 CPU profile：

```bash
curl http://127.0.0.1:6060/debug/pprof/profile?seconds=20 --output cpu.pprof
```

## 10. 建议怎么记录结果

把每次压测结果填写到：

- `docs/performance-report-template.md`

至少记录：

1. 本次机器配置
2. wrk 参数
3. video detail 三组数据
4. feed listLatest 三组数据
5. `/metrics` 和 `docker stats` 的关键观察

## 11. 最适合写进简历的结论

你最适合产出的不是“极限 QPS 很高”，而是：

1. 已完成 `wrk + metrics + pprof` 的可复现性能验证链路
2. 已对比视频详情与 Feed 读路径在不同缓存/回源模式下的性能表现
3. 为后续 Feed 优化、Like 写链路压测和 MQ 降级测试建立了基线
