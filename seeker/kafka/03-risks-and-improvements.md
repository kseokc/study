# 발생 가능한 문제와 개선 체크리스트

이 문서는 seeker의 현재 Kafka 방식에서 발생할 수 있는 문제와 개선 방향을 정리한다.

## 요약

현재 Kafka 구조는 개발/프로젝트 설명용으로는 충분히 타당하다. 하지만 운영 품질까지 고려하면 다음 네 가지를 반드시 봐야 한다.

1. 유실: Kafka publish 실패, broken message skip, queue/drop 등
2. 중복: producer retry, ClickHouse Kafka Engine 재처리, consumer commit 타이밍
3. 순서: 같은 trace 안의 이벤트 순서, cross-table 저장 순서
4. 운영성: topic 관리, lag monitoring, DLQ, schema versioning

## 1. Fire-and-forget publish로 인한 유실 가능성

현재 collector는 Kafka publish를 대부분 다음처럼 처리한다.

```java
producer.sendSomething(...).subscribe(null, err -> {});
```

장점:

- gRPC handler가 Kafka 응답을 기다리지 않는다.
- collector ingest latency가 낮다.
- Kafka 일시 지연이 agent에 바로 전파되지 않는다.

문제:

- Kafka publish 실패를 agent가 알 수 없다.
- gRPC 응답 성공과 Kafka 저장 성공이 같은 의미가 아니다.
- span은 성공했지만 span_event는 실패하는 partial write가 가능하다.
- 에러 consumer가 비어 있는 곳은 후속 조치가 없다.

개선:

- 최소한 publish 실패 counter를 추가한다.
- topic/key/eventType별 실패 로그를 일관되게 남긴다.
- 실패 메시지를 DLQ topic으로 보내는 전략을 검토한다.
- 중요한 lifecycle event는 retry 또는 ack 기다림 정책을 다르게 둘 수 있다.

공부 포인트:

> fire-and-forget은 latency를 낮추지만 delivery guarantee를 약하게 만든다.

## 2. Producer reliability 설정이 명시적이지 않음

현재 `application.yaml`에는 serializer만 명시되어 있다.

```yaml
spring.kafka.producer.key-serializer
spring.kafka.producer.value-serializer
```

운영에서 검토할 설정:

```yaml
spring:
  kafka:
    producer:
      acks: all
      retries: 10
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
        delivery.timeout.ms: 120000
        linger.ms: 5
        compression.type: lz4
```

설명:

| 설정 | 의미 |
| --- | --- |
| `acks=all` | leader뿐 아니라 ISR replica까지 write 확인 |
| `retries` | 일시 실패 시 재시도 |
| `enable.idempotence=true` | retry로 인한 중복 produce 위험 감소 |
| `delivery.timeout.ms` | producer가 전송을 포기하기까지의 최대 시간 |
| `linger.ms` | 짧게 기다려 batch를 키워 throughput 개선 |
| `compression.type` | 네트워크와 broker storage 사용량 감소 |

주의:

- local 단일 broker에서는 `acks=all`의 효과가 제한적이다.
- 운영에서 replication factor와 min.insync.replicas를 같이 설정해야 의미가 있다.

## 3. Topic 생성/partition/retention 정책 부재

현재 코드에서 `NewTopic`, `TopicBuilder` 같은 topic 생성 설정은 확인되지 않는다. 따라서 topic은 Kafka auto-create에 의존하거나 수동으로 만들어야 한다.

문제:

- partition 수가 의도와 다르게 만들어질 수 있다.
- retention 정책이 데이터 특성에 맞지 않을 수 있다.
- local에서는 동작하지만 다른 환경에서 topic이 없어 실패할 수 있다.

개선:

- Spring Kafka `NewTopic` bean 또는 별도 infra script로 topic을 명시한다.
- topic별 partition, replication factor, retention을 문서화한다.

예시 정책:

| Topic | Partition 기준 | Retention 기준 | 비고 |
| --- | --- | --- | --- |
| `trace-data` | trace ingest volume 기준 | ClickHouse 적재 지연을 버틸 만큼 | traceId key |
| `metric` | agent 수/metric volume 기준 | 짧아도 됨 | agentId key |
| `log` | 가장 큰 volume 예상 | 별도 retention 필요 | traceId 또는 agentId key |
| `agent-events` | 낮은 volume | 길게 보관 가능 | agent lifecycle |

## 4. 단일 broker / replication factor 1

현재 `docker-compose.yml`은 local 개발용 단일 Kafka다.

```text
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
```

이건 local 개발에서는 맞다. 단일 노드 KRaft에서 internal topic을 만들려면 replication factor 1이 필요하다.

하지만 운영에서는 위험하다.

문제:

- broker 장애 시 데이터 유실 가능성이 크다.
- replication이 없으므로 `acks=all`도 강한 내구성을 제공하지 못한다.
- controller와 broker가 같은 노드라 장애 격리가 없다.

개선:

- 운영 Kafka는 최소 3 broker를 검토한다.
- topic replication factor 3을 검토한다.
- `min.insync.replicas=2`, producer `acks=all` 조합을 검토한다.

## 5. ClickHouse Kafka Engine의 중복 가능성

ClickHouse Kafka Engine + Materialized View 방식은 ingest를 단순하게 만들지만, 장애 상황에서 중복 적재 가능성을 고려해야 한다.

가능한 상황:

- ClickHouse가 Kafka block을 읽고 storage table에 insert했다.
- commit 전에 ClickHouse 또는 network 문제가 생겼다.
- 같은 Kafka offset을 다시 읽어 같은 row가 다시 insert될 수 있다.

결과:

- trace/span/log row가 중복될 수 있다.
- count, latency 집계가 부풀 수 있다.

개선:

- 중복 허용 여부를 먼저 결정한다.
- unique key 역할을 할 id를 payload에 명확히 둔다.
- 필요하면 `ReplacingMergeTree` 또는 deduplication 전략을 검토한다.
- query에서 중복 제거가 필요한지 확인한다.

공부 포인트:

> Kafka consumer 기반 적재는 보통 at-least-once로 이해하고, downstream에서 idempotency를 설계해야 한다.

## 6. Poison message와 DLQ 부재

현재 ClickHouse Kafka source table에는 다음 설정이 있다.

```sql
kafka_skip_broken_messages = 100
```

장점:

- 일부 깨진 메시지 때문에 전체 consumer가 멈추는 것을 줄인다.

문제:

- 어떤 메시지가 깨졌는지 추적하기 어렵다.
- skip된 메시지는 데이터 유실이다.
- schema 변경 실수나 JSON 타입 오류가 조용히 누락될 수 있다.

개선:

- collector 단계에서 payload validation을 추가한다.
- Kafka publish 전 eventType별 required field를 검증한다.
- broken message 로그/metric을 수집한다.
- DLQ topic을 둔다. 예: `trace-data-dlq`, `metric-dlq`, `log-dlq`

## 7. Schema evolution 문제

현재 payload는 JSON이고 ClickHouse MV에서 `JSONExtract`로 필드를 꺼낸다.

장점:

- 개발이 빠르다.
- 필드 추가에 비교적 유연하다.

문제:

- 필드 이름 변경에 취약하다.
- 타입 변경에 취약하다.
- producer와 consumer가 같은 schema를 공유한다는 보장이 약하다.
- event version이 없다.

개선:

- `EventEnvelope`에 `schemaVersion`을 추가한다.
- eventType별 payload schema를 문서화한다.
- 필드 제거/타입 변경은 breaking change로 관리한다.
- 장기적으로는 Protobuf/Avro + Schema Registry도 검토할 수 있다.

예시:

```java
public class EventEnvelope<T> {
    private EventType eventType;
    private Long timestamp;
    private Integer schemaVersion;
    private T payload;
}
```

## 8. 순서에 대한 오해

Kafka는 partition 안의 순서를 보장한다. 전체 topic 순서를 보장하는 것은 아니다.

현재 `trace-data`는 `traceId`를 key로 사용하므로 같은 trace의 `TRACE`, `SPAN`, `SPAN_EVENT`는 같은 partition으로 들어갈 가능성이 높다.

하지만 주의할 점:

- 여러 topic 사이의 순서는 보장되지 않는다.
- `trace`, `span`, `span_event`는 서로 다른 ClickHouse table에 들어가므로 table 간 저장 순서는 보장하지 않는 편이 좋다.
- 현재 코드에서는 root span을 먼저 `SPAN`으로 보내고 그 다음 `TRACE`를 보낸다.

따라서 query나 UI는 다음을 전제로 하면 안 된다.

```text
TRACE row가 항상 SPAN row보다 먼저 존재한다.
```

대신 다음처럼 설계하는 편이 안전하다.

```text
eventual consistency: 잠깐 비어 있을 수 있지만 곧 들어온다.
```

## 9. Log topic volume 문제

log는 trace/span보다 volume이 훨씬 커질 수 있다.

현재 구조:

- agent는 log batch를 gRPC로 보낸다.
- collector는 `LogBatch` 안의 log를 하나씩 Kafka `log` topic에 발행한다.
- ClickHouse는 `kafka_log` source table에서 `log` table로 적재한다.

문제 가능성:

- log burst가 Kafka producer와 broker를 압박할 수 있다.
- collector에서 log 한 건마다 Kafka send를 호출하면 overhead가 커질 수 있다.
- ClickHouse consumer가 따라가지 못하면 lag가 쌓인다.

개선:

- Kafka producer `linger.ms`, `batch.size`, `compression.type` 조정
- log topic partition 수를 trace-data보다 크게 설정
- ClickHouse `kafka_num_consumers` 증가 검토
- log sampling 또는 severity filter 검토
- log retention을 trace/metric과 별도로 관리

## 10. 관측해야 할 metric

Kafka를 운영하려면 다음 metric을 봐야 한다.

Collector producer:

- send success count
- send failure count
- send latency
- retry count
- record error rate
- topic/key별 publish count

Kafka broker/topic:

- topic partition count
- consumer group lag
- under replicated partitions
- offline partitions
- request latency
- disk usage

ClickHouse consumer:

- Kafka consumer lag
- materialized view insert error
- broken message skip count
- insert throughput
- storage table row 증가량

Agent/collector 관점:

- gRPC ingest count
- Kafka publish count
- gRPC ingest count와 Kafka publish count 차이
- ClickHouse inserted row count

## 개선 우선순위

바로 하면 좋은 것:

1. topic 생성/partition/retention 정책 문서화
2. producer 설정 명시: `acks`, `retries`, `enable.idempotence`, `compression`
3. Kafka publish 실패 counter/log 정리
4. `EventEnvelope`에 `schemaVersion` 추가 검토
5. ClickHouse Kafka lag와 broken message 관측 추가

다음 단계:

1. DLQ topic 도입
2. duplicate 처리 전략 정리
3. log topic batch/partition 최적화
4. 운영 Kafka replication/min ISR 설정
5. schema registry 또는 protobuf 기반 Kafka payload 검토

## 최종 판단

현재 seeker의 Kafka 사용은 방향이 맞다.

특히 다음 선택은 타당하다.

- collector와 ClickHouse 사이에 Kafka를 둔 것
- trace, metric, log, agent event를 topic별로 나눈 것
- traceId/agentId를 key로 사용한 것
- ClickHouse Kafka Engine과 Materialized View로 비동기 적재한 것

하지만 현재 구현은 local/dev와 포트폴리오 설명에는 적합하지만, 운영 수준으로는 다음 보강이 필요하다.

- delivery guarantee 명시
- producer reliability 설정
- topic lifecycle 관리
- DLQ와 schema versioning
- duplicate/idempotency 대응
- lag/error/drop metric
