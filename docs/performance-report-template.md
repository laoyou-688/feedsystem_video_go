# Feed System Performance Report

## Test Profile

- Date:
- Commit:
- Environment:
- API instance count:
- Worker instance count:
- MySQL / Redis / RabbitMQ deployment:
- Load tool:
- Test duration / threads / connections:

## Observability Snapshot

- Metrics endpoint: `/metrics`
- Pprof endpoint:
- Key counters watched:
  - `feedsystem_http_requests_total`
  - `feedsystem_http_request_duration_seconds`
  - `feedsystem_http_in_flight_requests`
- Other evidence:
  - Redis command stats / hit ratio
  - MySQL slow query log / EXPLAIN
  - RabbitMQ queue depth

## Scenario 1: Video Detail Cache Path

| Path | QPS | Avg | P95 | P99 | Notes |
| --- | --- | --- | --- | --- | --- |
| Local cache |  |  |  |  |  |
| Redis cache |  |  |  |  |  |
| MySQL direct |  |  |  |  |  |

## Scenario 2: Feed List

| Endpoint | Cache / Mode | QPS | Avg | P95 | P99 | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `/feed/listLatest` |  |  |  |  |  |  |
| `/feed/listByPopularity` |  |  |  |  |  |  |

## Scenario 3: Like Write Path

| Mode | QPS | Avg | P95 | P99 | Error Rate | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| RabbitMQ enabled |  |  |  |  |  |  |
| RabbitMQ disabled / degraded |  |  |  |  |  |  |

## Findings

1. Cache benefit:
2. MQ async write benefit:
3. Slowest bottleneck:
4. Redis / MySQL pressure characteristics:

## Follow-up Improvements

1. Add endpoint-specific metrics for feed / like / comment.
2. Add Prometheus + Grafana in `docker-compose`.
3. Add scripted EXPLAIN snapshots for hot SQL.
4. Add repeatable like-path pressure scripts and degradation test cases.
