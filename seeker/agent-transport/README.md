# seeker-agent 전송 구조 학습

이 폴더는 `seeker-agent`에서 기존 동기 전송 방식 대신 gRPC 기반 비동기 전송 방식을 선택한 이유를 정리한다.

읽는 순서:

1. [동기 전송 병목과 비동기 gRPC 전송 선택 이유](./01-sync-vs-async-grpc.md)
2. [현재 seeker-agent 코드 기준 전송 구조](./02-code-evidence.md)
3. [포트폴리오와 면접 답변 정리](./03-portfolio-talking-points.md)

핵심 한 문장:

> APM agent는 관측 도구이므로 사용자 요청 흐름을 막으면 안 된다. 그래서 collector 전송을 request thread에서 분리하고, bounded queue와 worker thread, gRPC stream으로 비동기 전송하도록 설계했다.
