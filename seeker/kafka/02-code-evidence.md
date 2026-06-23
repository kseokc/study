# 현재 코드 기준 Kafka 구현 근거

이 문서는 seeker 코드에서 Kafka가 어떻게 구현되어 있는지 정리한다.

## 전체 경로

```text
seeker-agent
  -> gRPC stream
  -> seeker-collector
  -> Kafka producer
  -> Kafka topic
  -> ClickHouse Kafka Engine table
  -> Materialized View
  -> MergeTree storage table
```

관련 파일:

- `/Users/gimseogchan/dev/seeker/seeker-collector/src/main/java/com/seeker/collector/grpc/CollectorGrpcService.java`
- `/Users/gimseogchan/dev/seeker/seeker-collector/src/main/java/com/seeker/collector/otlp/OtlpTraceService.java`
- `/Users/gimseogchan/dev/seeker/seeker-collector/src/main/java/com/seeker/collector/kafka/producer/KafkaEventPublisher.java`
- `/Users/gimseogchan/dev/seeker/seeker-clickhouse/sql/kafka_source.sql`
- `/Users/gimseogchan/dev/seeker/seeker-clickhouse/sql/materialized_views.sql`

## 1. KafkaEventPublisher

파일:

```text
/Users/gimseogchan/dev/seeker/seeker-collector/src/main/java/com/seeker/collector/kafka/producer/KafkaEventPublisher.java
```

핵심 코드:

```java
public <T> Mono<Void> publish(String topic, String key, EventEnvelope<T> event) {

    return Mono.fromFuture(kafkaTemplate.send(topic, key, event))
            .doOnError(ex ->
                    log.error("[Kafka] event 전송 실패 topic={}, key={}, eventType={}",
                            topic,
                            key,
                            event.getEventType(),
                            ex)
            )
            .then();
}
```

의미:

- `KafkaTemplate<String, Object>`로 Kafka에 JSON event를 보낸다.
- topic과 key를 호출자가 결정한다.
- send 결과는 `Mono<Void>`로 감싼다.
- 실패하면 log를 남긴다.

중요한 점:

- `publish()`는 `Mono`를 반환할 뿐, 이 메서드 자체에서 block하지 않는다.
- 실제 발행을 기다릴지는 호출자가 `subscribe`, `block`, reactive chain 연결 중 무엇을 하느냐에 달려 있다.
- 현재 gRPC handler에서는 대부분 `subscribe(null, err -> {})` 형태로 fire-and-forget 처리한다.

## 2. trace-data topic producer

파일:

```text
/Users/gimseogchan/dev/seeker/seeker-collector/src/main/java/com/seeker/collector/kafka/producer/TraceDataKafkaProducer.java
```

핵심:

```java
private static final String TRACE_DATA_TOPIC = "trace-data";
```

```java
public Mono<Void> sendTrace(TracePayload payload, String traceId) {
    return sendEvent(EventType.TRACE, traceId, payload);
}

public Mono<Void> sendSpan(SpanPayload payload, String traceId) {
    return sendEvent(EventType.SPAN, traceId, payload);
}

public Mono<Void> sendSpanEvent(SpanEventPayload payload, String traceId) {
    return sendEvent(EventType.SPAN_EVENT, traceId, payload);
}
```

의미:

- `TRACE`, `SPAN`, `SPAN_EVENT`를 모두 `trace-data` topic으로 보낸다.
- key는 `traceId`다.
- 같은 trace의 이벤트를 같은 partition에 모으려는 설계다.

## 3. metric, log, agent topic producer

Agent:

```text
topic: agent-events
key  : agentId
event: AGENT_CREATED, AGENT_DELETED
```

Metric:

```text
topic: metric
key  : agentId
event: METRIC_SNAPSHOT
```

Log:

```text
topic: log
key  : traceId if exists, otherwise agentId
event: LOG
```

`LogKafkaProducer`의 key 선택:

```java
private String resolveKey(LogPayload payload) {
    if (payload.getTraceId() != null && !payload.getTraceId().isBlank()) {
        return payload.getTraceId();
    }
    return payload.getAgentId();
}
```

이 선택은 적절하다.

이유:

- trace가 있는 로그는 trace detail과 묶어서 볼 가능성이 높다.
- trace가 없는 로그도 agent 기준으로 partitioning할 수 있다.

## 4. collector gRPC handler에서 Kafka publish

파일:

```text
/Users/gimseogchan/dev/seeker/seeker-collector/src/main/java/com/seeker/collector/grpc/CollectorGrpcService.java
```

Span 처리:

```java
traceDataKafkaProducer.sendSpan(spanPayload, traceId)
        .subscribe(null, err -> {});

if (spanPayload.getParentSpanId() == -1) {
    traceDataKafkaProducer.sendTrace(toTracePayload(span), traceId)
            .subscribe(null, err -> {});
}
```

SpanEvent 처리:

```java
traceDataKafkaProducer.sendSpanEvent(spanEventPayload, traceId)
        .subscribe(null, err -> {});
```

Metric 처리:

```java
metricKafkaProducer.sendMetricSnapshot(metricSnapshotPayload, metricSnapshotPayload.getAgentId())
        .subscribe(null, err ->{});
```

Log 처리:

```java
logKafkaProducer.sendLog(logPayload)
        .subscribe(null, err -> {});
```

의미:

- collector는 agent로부터 받은 gRPC stream 처리 중 Kafka publish를 비동기로 시작한다.
- Kafka publish 완료를 기다리지 않는다.
- Kafka 실패가 gRPC stream을 실패시키지 않는다.

장점:

- collector의 gRPC 처리 latency가 낮아진다.
- Kafka 장애가 agent gRPC stream에 바로 전파되지 않는다.
- 관측 파이프라인에서 collector가 빠르게 ingest 역할을 수행할 수 있다.

위험:

- Kafka publish 실패를 agent가 알 수 없다.
- 일부 span만 성공하고 trace 또는 span_event가 실패하는 partial write가 가능하다.
- `subscribe(null, err -> {})`에서 에러 consumer가 비어 있어 publisher의 `doOnError` 로그 외에 후속 처리가 없다.

## 5. OTLP trace service도 fire-and-forget

파일:

```text
/Users/gimseogchan/dev/seeker/seeker-collector/src/main/java/com/seeker/collector/otlp/OtlpTraceService.java
```

문서 주석에 명시되어 있다.

```text
Kafka 발행은 비동기 fire-and-forget.
발행 실패해도 클라이언트에는 성공으로 응답한다.
```

코드도 같은 방향이다.

```java
traceDataKafkaProducer.sendSpan(spanPayload, traceId)
        .subscribe(null, err -> log.error(
                "[OTLP] SPAN 발행 실패 - traceId: {}", traceId, err));
```

의미:

- OTLP client에게는 빠르게 성공 응답을 준다.
- Kafka publish 성공 여부는 응답 의미에 포함되지 않는다.

## 6. Spring Kafka producer 설정

파일:

```text
/Users/gimseogchan/dev/seeker/seeker-collector/src/main/resources/application.yaml
```

현재 설정:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
```

명시된 것:

- bootstrap server
- key serializer
- value serializer

명시되지 않은 것:

- `acks`
- `retries`
- `enable.idempotence`
- `linger.ms`
- `batch.size`
- `compression.type`
- `delivery.timeout.ms`
- topic 생성 정책
- partition 수
- retention 정책

따라서 현재 설정은 local/dev 환경에는 충분하지만, 운영 기준으로는 producer reliability와 throughput 설정을 명시하는 편이 좋다.

## 7. Docker Compose Kafka

파일:

```text
/Users/gimseogchan/dev/seeker/docker-compose.yml
```

현재 Kafka:

```yaml
image: apache/kafka:latest
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_ADVERTISED_LISTENERS: HOST://localhost:9092,INTERNAL://seeker-kafka:29092
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

의미:

- 단일 노드 KRaft Kafka다.
- host에서는 `localhost:9092`로 접근한다.
- compose network 안의 ClickHouse는 `seeker-kafka:29092`로 접근한다.
- replication factor는 local 단일 노드에 맞춰 1이다.

주의:

- `apache/kafka:latest`는 재현성 측면에서 버전을 고정하는 편이 좋다.
- replication factor 1은 broker 장애 시 데이터 보호가 없다.
- 운영 환경에서는 broker 3대 이상과 replication factor 3을 검토해야 한다.

## 8. ClickHouse Kafka Engine source table

파일:

```text
/Users/gimseogchan/dev/seeker/seeker-clickhouse/sql/kafka_source.sql
```

예시:

```sql
CREATE TABLE IF NOT EXISTS kafka_trace_data
(
    eventType LowCardinality(String),
    timestamp Int64,
    payload   String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'seeker-kafka:29092',
    kafka_topic_list = 'trace-data',
    kafka_group_name = 'clickhouse-trace-consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_skip_broken_messages = 100,
    kafka_thread_per_consumer = 1,
    input_format_import_nested_json = 1,
    input_format_json_read_objects_as_strings = 1;
```

의미:

- ClickHouse가 Kafka consumer 역할을 한다.
- Kafka 메시지를 `JSONEachRow`로 읽는다.
- `payload`는 raw JSON string으로 읽고 materialized view에서 파싱한다.
- topic별 consumer group을 분리했다.

주의:

- `kafka_num_consumers = 1`이라 ClickHouse 소비 병렬성은 낮다.
- topic partition을 늘려도 ClickHouse consumer가 1이면 소비 확장 효과가 제한된다.
- `kafka_skip_broken_messages = 100`은 깨진 메시지를 일부 skip할 수 있으므로 데이터 유실을 감지할 방법이 필요하다.

## 9. Materialized View fan-out

파일:

```text
/Users/gimseogchan/dev/seeker/seeker-clickhouse/sql/materialized_views.sql
```

`trace-data` topic은 eventType으로 fan-out한다.

```sql
FROM kafka_trace_data
WHERE eventType = 'TRACE';
```

```sql
FROM kafka_trace_data
WHERE eventType = 'SPAN';
```

```sql
FROM kafka_trace_data
WHERE eventType = 'SPAN_EVENT';
```

`metric`은 snapshot의 `points[]`를 `ARRAY JOIN`으로 row 단위로 펼친다.

```sql
ARRAY JOIN JSONExtract(
    payload, 'points',
    'Array(Tuple(metricName String, fieldName String, value Float64, type String, tags Map(String, String)))'
) AS point
```

의미:

- Kafka topic은 이벤트 단위로 받고, ClickHouse 저장 테이블은 조회 목적에 맞게 정규화한다.
- `trace`, `span`, `span_event`, `metric`, `log` table을 각각 따로 조회 최적화할 수 있다.

## 현재 구현 판단

현재 구현은 seeker의 목적에 맞는 방향이다.

좋은 선택:

- collector와 ClickHouse를 Kafka로 분리했다.
- topic을 데이터 성격별로 나눴다.
- traceId/agentId를 key로 사용했다.
- ClickHouse Kafka Engine과 Materialized View로 ingest를 단순화했다.
- collector의 gRPC 처리 path에서 DB insert를 제거했다.

보강이 필요한 선택:

- Kafka publish 실패를 상위 호출자가 알 수 없다.
- producer reliability 설정이 명시적이지 않다.
- topic 생성/partition/retention이 코드로 관리되지 않는다.
- ClickHouse consumer lag, skip count, MV failure 관측이 부족하다.
- 중복 적재와 partial write에 대한 정책이 없다.
