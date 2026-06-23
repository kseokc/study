# seeker Kafka 학습

이 폴더는 `seeker`에서 Kafka를 왜 사용했는지, 현재 구현이 목적에 맞는지, 어떤 문제가 생길 수 있는지 정리한다.

읽는 순서:

1. [seeker에서 Kafka를 사용하는 이유와 전체 흐름](./01-why-kafka-in-seeker.md)
2. [현재 코드 기준 Kafka 구현 근거](./02-code-evidence.md)
3. [발생 가능한 문제와 개선 체크리스트](./03-risks-and-improvements.md)

핵심 한 문장:

> seeker에서 Kafka는 collector와 ClickHouse 사이를 분리하는 이벤트 버퍼다. agent에서 들어오는 trace, metric, log를 collector가 즉시 저장소에 직접 쓰지 않고 Kafka topic에 발행하면, ClickHouse가 Kafka Engine과 Materialized View로 비동기 적재한다.
