# seeker에서 ClickHouse를 사용하는 이유

## 결론

현재 seeker-clickhouse 용도에는 ClickHouse를 쓰는 판단이 타당하다.

다만 이유를 "ClickHouse가 시계열 DB라서"라고만 설명하면 조금 부정확하다. 더 정확히는 ClickHouse가 로그, 트레이스, 메트릭 같은 대량의 append-only observability 데이터를 컬럼형 저장 구조와 SQL로 빠르게 분석하기 좋은 OLAP 데이터베이스이기 때문이다.

정리하면 다음 문장이 핵심이다.

> ClickHouse를 선택하는 이유는 전용 시계열 DB라서가 아니라, 대량의 시간 기반 observability 이벤트를 컬럼형 저장, MergeTree, TTL, materialized view, Kafka ingest, SQL 분석, text index로 함께 처리할 수 있기 때문이다.

## ClickHouse의 정체

ClickHouse는 전통적인 의미의 전용 시계열 DB라기보다 컬럼 지향 OLAP 데이터베이스다.

공식 문서에서도 ClickHouse를 logs, traces, metrics 같은 observability 데이터의 주요 저장/분석 수단으로 설명한다. ClickStack도 ClickHouse와 OpenTelemetry 기반으로 logs, traces, metrics를 통합하는 observability stack으로 소개된다.

또한 ClickHouse는 time-series 데이터에도 강점을 가진다. 시스템 메트릭, 애플리케이션 로그, 비즈니스 이벤트처럼 시간에 따라 쌓이는 데이터를 저장하고 분석하는 기능을 제공한다.

즉 ClickHouse는 "시계열도 잘 처리하는 컬럼형 OLAP DB"에 가깝다.

## 일반 RDBMS와 다른 핵심 구조

PostgreSQL이나 MySQL 같은 일반 RDBMS는 보통 row storage, B-tree index, transaction, update, relational join에 강하다. 반면 ClickHouse는 분석형 append-only 데이터에 맞춰져 있다.

주요 차이는 다음과 같다.

- 컬럼 단위로 저장한다. 그래서 `timestamp`, `trace_id`, `service_name`, `severity_number`처럼 일부 컬럼만 읽는 분석 쿼리에 유리하다.
- `MergeTree` 계열 엔진이 대량 insert에 맞춰져 있다. insert된 데이터는 part로 쌓이고 백그라운드에서 merge된다.
- primary index가 모든 row를 직접 가리키는 B-tree가 아니다. 정렬된 데이터 위에 granule 단위로 잡히는 sparse primary index다.
- 기본 `index_granularity`는 8192다. 즉 기본적으로 8192행 단위 mark를 사용한다.
- 인덱스가 작고 메모리에 올리기 좋지만, 단건 row lookup용 OLTP DB처럼 동작하지는 않는다.

이 구조는 "최근 N분/시간/일 데이터 집계", "서비스별 latency", "trace detail 조회", "로그 필터링" 같은 observability 쿼리에 잘 맞는다.

## seeker 프로젝트 구조와 맞는 부분

현재 seeker의 ClickHouse 스키마는 ClickHouse식 설계에 가깝다.

Kafka source table:

- `kafka_trace_data`
- `kafka_agent_events`
- `kafka_metric`
- `kafka_log`

Materialized View:

- Kafka envelope를 파싱해서 저장 테이블로 적재한다.
- source table과 storage table 사이의 변환 계층 역할을 한다.

Storage table:

- `trace`
- `span`
- `span_event`
- `agent`
- `metric`
- `log`

공통적으로 보이는 설계:

- `ENGINE = MergeTree`
- `PARTITION BY toYYYYMMDD(...)` 또는 `toYYYYMM(...)`
- `TTL ... + INTERVAL 30 DAY`
- 조회 패턴에 맞춘 `ORDER BY (...)`

이 구조는 계속 들어오는 이벤트를 Kafka에서 받아 append-only로 저장하고, 최근 기간 기준 조회와 trace detail, service별 집계를 수행하는 워크로드에 적합하다.

## 현재 스키마별 해석

`trace`:

```sql
PARTITION BY toYYYYMMDD(start_time)
ORDER BY (start_time, trace_id)
TTL toDateTime(start_time) + INTERVAL 30 DAY
```

최근 시간 범위 기준 trace 목록을 조회하기 좋다. trace 목록 화면, 시간 범위 필터, 정렬 기반 조회에 맞는 설계다.

`span`:

```sql
PARTITION BY toYYYYMMDD(start_time)
ORDER BY (trace_id, span_id, start_time)
TTL toDateTime(start_time) + INTERVAL 30 DAY
```

`trace_id`로 특정 trace의 span 전체를 찾는 detail 조회에 좋다. `TraceViewRepository`의 주석처럼 `trace_id` 단일 lookup을 빠르게 하기 위한 정렬키다.

`span_event`:

```sql
PARTITION BY toYYYYMMDD(start_time)
ORDER BY (span_id, sequence)
TTL toDateTime(start_time) + INTERVAL 30 DAY
```

span별 event를 순서대로 가져오기 좋다. trace detail에서 span 목록을 구한 뒤 `span_id IN (...)`으로 event를 가져오는 패턴에 맞다.

`metric`:

```sql
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (agent_id, metric_name, field_name, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
```

agent, metric name, field 단위로 시간 버킷 집계를 하는 쿼리에 맞다. 특정 agent의 특정 metric을 시간 순서로 그리는 대시보드에 적합하다.

`log`:

```sql
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (trace_id, span_id, timestamp, severity_number)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
```

trace detail에서 특정 trace의 로그를 시간순으로 보여주는 조회에 좋다.

```sql
WHERE trace_id = '<trace-id>'
ORDER BY timestamp
```

반면 다음과 같은 로그 대시보드 쿼리에는 상대적으로 덜 유리할 수 있다.

```sql
WHERE timestamp >= now() - INTERVAL 10 MINUTE
GROUP BY service_name, severity_text
```

정렬키의 첫 컬럼이 `trace_id`라서 "최근 시간 범위 전체 로그"를 훑는 쿼리는 `timestamp`나 `service_name` 중심 정렬키보다 효율이 떨어질 수 있다. 그래도 `PARTITION BY toYYYYMMDD(timestamp)` 덕분에 날짜 단위 partition pruning은 가능하다.

## 로그 검색과 text index

ClickHouse에는 full-text search용 text index가 있다. 공식 문서 기준 text index는 inverted index 계열이며, token을 token이 포함된 row 위치에 매핑한다. `hasToken`, `hasAllTokens`, `hasAnyTokens`, `hasPhrase` 같은 함수로 검색할 수 있다.

ClickHouse text index가 좋은 경우:

- 로그 본문에서 특정 단어 또는 패턴을 찾는다.
- 검색 결과를 곧바로 SQL 집계와 엮는다.
- 시간 필터, service 필터, severity 필터, `count`, `group by`가 함께 중요하다.

Elasticsearch나 OpenSearch가 더 나은 경우:

- relevance ranking이 중요하다.
- 복잡한 analyzer가 필요하다.
- 한국어 형태소 분석 품질이 핵심이다.
- 자동완성, highlight, fuzzy search, 검색 UX 튜닝이 제품의 중심이다.

따라서 seeker가 APM/observability 제품이라면 ClickHouse text index는 충분히 고려할 만하다. 하지만 검색 서비스 자체가 핵심 제품이라면 별도 검색 엔진이 더 적합하다.

## 다른 시계열 DB와 비교

ClickHouse가 유리한 경우:

- 로그, 트레이스, 메트릭을 한 DB에서 함께 저장한다.
- 데이터 양이 많고 보존 기간이 정해져 있다.
- insert가 대부분이고 update/delete는 거의 없다.
- `GROUP BY service_name`, `GROUP BY severity_text`, `WHERE trace_id = ...`, p95 latency by endpoint 같은 분석 쿼리가 많다.
- Kafka에서 바로 ingest하고 싶다.
- raw data와 rollup/materialized view를 함께 운영하고 싶다.

다른 선택지가 더 나은 경우:

- Prometheus식 metric scraping, alert rule, PromQL 호환성이 핵심이면 Prometheus, Mimir, VictoriaMetrics 계열이 편하다.
- PostgreSQL 생태계, transaction, relational join이 중요하면 TimescaleDB가 편하다.
- 검색 품질과 검색 UX가 핵심이면 Elasticsearch 또는 OpenSearch가 낫다.
- 단건 수정/삭제, 강한 제약조건, FK, OLTP transaction이 중요하면 ClickHouse는 맞지 않는다.

## 운영하면서 볼 체크포인트

로그 대시보드 쿼리가 많아지면 `log` 테이블의 `ORDER BY (trace_id, span_id, timestamp, severity_number)`만으로는 부족할 수 있다. 이 정렬키는 trace detail에는 좋지만, 전체 로그를 최근 시간 기준으로 집계하는 쿼리에는 최적화되어 있지 않다.

그때 고려할 수 있는 선택지는 다음과 같다.

- 로그 대시보드용 별도 테이블: `ORDER BY (service_name, timestamp, severity_number, trace_id)`
- projection 추가
- `body` 컬럼에 text index 추가
- `timestamp`, `service_name`, `severity_text` 기준 materialized rollup table 추가
- metric alerting/PromQL 요구가 강해지면 Prometheus 계열 병행

## 최종 판단

seeker-agent와 seeker-collector에서 생성되는 trace, span, metric, log를 저장하고 observability 관점으로 조회/분석하는 목적이라면 ClickHouse는 맞는 선택이다.

현재 스키마도 Kafka ingest, materialized view, MergeTree, partition, TTL, query-oriented `ORDER BY`를 사용하고 있어서 ClickHouse의 장점을 잘 활용하는 방향이다.

단, ClickHouse를 "시계열 DB라서 선택했다"고만 설명하기보다는 "대량 observability 이벤트를 SQL로 빠르게 분석하기 좋은 컬럼형 OLAP DB라서 선택했다"고 이해하는 편이 정확하다.

## 참고 자료

- [ClickHouse Observability docs](https://clickhouse.com/docs/use-cases/observability)
- [ClickHouse Time-Series docs](https://clickhouse.com/docs/use-cases/time-series)
- [ClickHouse MergeTree engine](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree)
- [ClickHouse sparse primary indexes](https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes#clickhouse-index-design)
- [ClickHouse full-text search with text indexes](https://clickhouse.com/docs/engines/table-engines/mergetree-family/textindexes)
- local reference: `/Users/gimseogchan/dev/seeker/seeker-clickhouse/sql/storage_tables.sql`
- local reference: `/Users/gimseogchan/dev/seeker/seeker-clickhouse/sql/kafka_source.sql`
- local reference: `/Users/gimseogchan/dev/seeker/seeker-clickhouse/sql/materialized_views.sql`
