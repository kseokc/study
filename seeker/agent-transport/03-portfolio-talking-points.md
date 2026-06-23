# 포트폴리오와 면접 답변 정리

이 문서는 `seeker-agent/docs/포트폴리오/apm-agent-portfolio-points.md`의 내용을 공부/발표용으로 다시 정리한 것이다.

## 한 줄 요약

```text
Java Agent와 ByteBuddy 기반으로 애플리케이션 코드를 수정하지 않고 요청 흐름, 메서드 실행 시간, JDBC 쿼리, 외부 HTTP 호출, JVM 메트릭을 수집하는 APM Agent를 구현했습니다. 또한 동기 Collector 전송 병목을 bounded queue와 gRPC streaming 기반 비동기 전송 구조로 개선해 사용자 요청 처리 흐름과 모니터링 데이터 전송을 분리했습니다.
```

## 핵심 어필 포인트

가장 강하게 가져갈 포인트:

1. ByteBuddy 기반 Java Agent 계측 구조 구현
2. W3C Trace Context 기반 분산 추적 구현
3. 동기 Collector 전송 병목을 비동기 gRPC streaming 구조로 개선
4. Plugin 기반 확장형 instrumentation 구조 설계

보조 포인트:

- JDBC SQL 추적
- JVM metric scheduler 구현
- Collector 장애 격리
- debug mode 제공
- agent lifecycle/shutdown hook 관리

## 전송 구조 개선을 설명하는 답변

짧은 답변:

```text
초기에는 요청 종료 시점에 collector로 trace를 동기 전송했기 때문에 collector 지연이 사용자 요청 latency에 직접 포함되는 문제가 있었습니다. APM agent는 관측 도구라 비즈니스 요청을 막으면 안 된다고 판단했고, request thread는 span을 bounded queue에 넣고 즉시 반환하도록 바꿨습니다. 실제 전송은 별도 daemon worker가 gRPC stream으로 처리하게 해서 사용자 요청 흐름과 관측 데이터 전송 흐름을 분리했습니다.
```

조금 더 기술적인 답변:

```text
Trace.finish()는 DataSenderHolder를 통해 sender 인터페이스에 span을 넘기고, bootstrap 시점에 SenderModule이 AsyncSpanDispatcher를 주입합니다. AsyncSpanDispatcher는 MPSC bounded queue에 span을 적재하고, Seeker-DataSender-Worker가 queue에서 꺼내 GrpcSpanTransport로 전달합니다. GrpcSpanTransport는 gRPC async stub과 StreamObserver를 사용해 collector stream에 onNext로 데이터를 전송합니다. 이 구조로 request thread에서 collector network I/O를 제거했습니다.
```

성과 중심 답변:

```text
동기 HTTP 전송 구조를 queue 기반 비동기 gRPC streaming 구조로 개선한 결과, 포트폴리오 문서 기준 평균 응답 시간을 69ms에서 43ms로 약 38% 단축했고 API 처리량을 2배 이상 향상시켰습니다. 핵심은 collector 처리량이 애플리케이션 요청 처리량을 직접 제한하지 않도록 분리한 것입니다.
```

주의해서 말할 점:

- trace 전송을 "batch 전송"이라고 표현하면 현재 코드 기준 부정확할 수 있다.
- 정확한 표현은 "trace 전송은 bounded queue + sender worker + gRPC streaming"이다.
- metric은 "scheduler + batch flush + gRPC stream"이라고 말할 수 있다.
- log는 "bounded queue + batch flush + gRPC transport"에 가깝다.

## 면접 예상 질문과 답변

### Q1. 왜 동기 전송이 문제였나요?

동기 전송에서는 request thread가 collector 응답 또는 timeout을 기다립니다. Collector는 외부 시스템이라 네트워크 지연, overload, 재시작, backend 저장소 지연이 생길 수 있습니다. 이 지연이 사용자 API latency에 직접 포함되면 APM agent가 비즈니스 요청의 병목이 됩니다.

### Q2. 왜 그냥 thread pool로 보내지 않고 bounded queue를 뒀나요?

비동기화만 하고 무한히 쌓으면 collector 장애 시 agent가 대상 애플리케이션 메모리를 계속 사용합니다. bounded queue는 request thread와 sender worker 사이의 완충 역할을 하면서 메모리 사용 상한을 둡니다. APM에서는 애플리케이션 안정성을 우선해야 하므로, 극단적인 상황에서는 일부 span drop을 감수하는 설계가 더 현실적입니다.

### Q3. queue가 꽉 차면 어떻게 되나요?

현재 span 전송은 `queue.offer(span)`이 실패하면 drop될 수 있습니다. 다만 현재 span 쪽은 drop count metric/log가 부족합니다. 그래서 운영 품질을 높이려면 `droppedSpanCount`, queue size, send error count 같은 self metric을 추가하는 것이 다음 개선점입니다. log dispatcher에는 `droppedCount`가 이미 있습니다.

### Q4. 왜 gRPC stream을 선택했나요?

Trace, metric, log는 지속적으로 발생하는 관측 이벤트입니다. gRPC stream은 이런 연속 이벤트 전송에 잘 맞고, protobuf schema로 agent와 collector 사이의 메시지 계약을 명확히 할 수 있습니다. 또한 channel을 공유하고 stream을 분리해 trace, metric, log 전송 경로를 구성할 수 있습니다.

### Q5. 이 구조에서 장애 격리는 어떻게 되나요?

Request thread는 collector 전송을 직접 수행하지 않고 queue offer까지만 합니다. Collector 장애나 stream 오류는 `GrpcSpanTransport`와 sender worker 쪽에서 처리되며 business exception으로 전파되지 않습니다. 따라서 장애 영향은 요청 실패보다 관측 데이터 유실 또는 전송 실패로 제한됩니다.

### Q6. 현재 구조의 한계는 무엇인가요?

운영 품질 측면에서는 보강할 부분이 있습니다. span queue overflow 관측이 부족하고, gRPC reconnect/backoff 정책이 단순하며, shutdown 시 queue flush 보장이 약합니다. 또 agent는 대상 애플리케이션 안에서 동작하므로 gRPC, Netty, Protobuf, Guava dependency 충돌을 막기 위한 shading/relocation도 중요합니다.

## 포트폴리오 문장 모음

전송 구조:

```text
Collector 전송 지연이 사용자 요청 latency로 전파되는 문제를 해결하기 위해, 요청 처리 스레드와 데이터 전송 책임을 분리했습니다. bounded queue와 별도 sender worker thread를 도입해 요청 스레드는 span을 queue에 적재한 뒤 즉시 반환하고, 실제 전송은 gRPC streaming 기반 비동기 구조로 처리하도록 개선했습니다.
```

Agent 안정성:

```text
APM Agent는 대상 애플리케이션 내부에서 동작하므로, collector 장애가 비즈니스 요청 처리에 영향을 주지 않도록 장애 격리를 중요한 설계 원칙으로 두었습니다.
```

구현 근거:

```text
Trace.finish()는 DataSender 인터페이스에만 의존하고, SenderModule이 AsyncSpanDispatcher와 GrpcSpanTransport를 조립해 주입합니다. 이를 통해 core 도메인과 전송 구현을 분리하고, debug mode에서는 console sender로 교체할 수 있게 했습니다.
```

성과:

```text
동기 HTTP 전송 구조를 queue 기반 비동기 gRPC streaming 구조로 개선한 결과, 평균 응답 시간을 69ms에서 43ms로 약 38% 단축했고 API 처리량을 2배 이상 향상시켰습니다.
```

한계와 개선:

```text
현재 구조는 request path에서 collector I/O를 제거했지만, span drop count, retry/backoff, shutdown flush, self metric 같은 운영 관측성은 추가 개선 대상으로 보고 있습니다.
```

## 개념 키워드 정리

| 키워드 | 의미 | seeker-agent에서의 적용 |
| --- | --- | --- |
| Java Agent | JVM 시작 시 `-javaagent`로 붙어 bytecode를 조작하는 방식 | `premain()`에서 agent bootstrap |
| ByteBuddy | 런타임 bytecode instrumentation 라이브러리 | Tomcat/JDBC/HTTP client/service method 계측 |
| Trace | 하나의 요청 흐름 전체 | `Trace`가 root span과 span event를 관리 |
| Span | 요청 흐름의 처리 단위 | collector로 전송되는 핵심 trace 데이터 |
| SpanEvent | span 내부의 세부 이벤트 | method, JDBC, HTTP call 같은 하위 작업 |
| W3C Trace Context | 서비스 간 trace 연결 표준 | `traceparent`, `tracestate` propagation |
| Bounded queue | 크기가 제한된 queue | span/log를 request path에서 분리하며 메모리 상한 설정 |
| MPSC queue | multi-producer single-consumer queue | 여러 request thread, 하나의 sender worker 구조 |
| gRPC stream | 하나의 RPC stream에 메시지를 연속 전송 | span/metric/log 이벤트 전송 |
| Backpressure | 생산 속도가 소비 속도보다 빠를 때의 압력 제어 | 현재는 queue capacity와 drop으로 단순 처리 |

## 발표 흐름 예시

```text
1. Java Agent라 대상 서비스 내부에서 실행된다.
2. 그래서 agent가 사용자 요청을 막지 않는 것이 가장 중요했다.
3. 초기 동기 collector 전송은 collector 지연을 request latency로 전파했다.
4. 이를 bounded queue와 sender worker로 분리했다.
5. 실제 전송은 gRPC async stub + stream으로 처리했다.
6. 그 결과 request thread에서는 network I/O가 제거됐다.
7. 다만 drop 관측, backoff, shutdown flush는 운영 품질 개선 과제로 남아 있다.
```

## 관련 원본 문서

- `/Users/gimseogchan/dev/seeker/seeker-agent/docs/포트폴리오/apm-agent-portfolio-points.md`
- `/Users/gimseogchan/dev/seeker/seeker-agent/docs/troubleshooting-sync-collector-bottleneck.md`
- `/Users/gimseogchan/dev/seeker/seeker-agent/docs/agent-open-source-analysis.md`
- `/Users/gimseogchan/dev/seeker/seeker-agent/docs/limitations.md`
