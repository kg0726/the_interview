# 💽 The Interview

> AI가 질문하고, 사용자는 음성으로 답하는 AI 기반 실시간 음성 인터뷰 서비스

🏆 **삼성 SSAFY 14기 프로젝트 우수상** | 5인 팀 | 2026.02 ~ 03

📄 **[상세 포트폴리오](https://www.notion.so/PROJECT_PORTFOLIO-3370380ca5bd80429e39dd223b7d99d2)**

---

## 담당 역할 (Backend)

- STOMP 기반 WebSocket 인터뷰 음성 입출력 전 과정 설계 및 구현
- 오디오 청크 단위 STT 변환 및 순서 보장 파이프라인 구현
- Facade 패턴 도입으로 트랜잭션 분리 및 비동기 체이닝 구조 설계
- Redis 기반 동시성 제어 및 Race Condition 해결

---

## 기술 스택

| Category | Stack |
|---|---|
| Backend | Java, Spring Boot, Spring WebFlux (WebClient), WebSocket (STOMP) |
| Database | PostgreSQL, Redis |
| AI | FastAPI, OpenAI Whisper (STT), Claude, Gemini |
| Infra | AWS EC2, Docker, Docker Compose, Nginx, Jenkins |
| Monitoring | Grafana, Prometheus, Loki |

---

## 핵심 기술 문제와 해결 과정

처음 만든 구조는 동작했지만, 실사용이 불가능한 수준이었습니다.
**8~10초의 응답 지연을 4초로 줄이기까지 세 번의 구조 개선**이 있었습니다.

<br>

### V1 → V2 : DB 커넥션 고갈 문제 + STT 지연 해결

**문제 1. `@Transactional` 범위 안에 AI 호출이 포함**

AI 서버 호출은 수 초가 걸리는 네트워크 I/O입니다.
그 시간 동안 DB 커넥션이 그대로 묶혀 있었고, 동시 사용자가 늘수록 커넥션 풀이 고갈될 구조였습니다.

같은 빈 내부에서 메서드를 분리해 호출하면 Spring AOP 프록시를 우회해 트랜잭션이 무효화된다는 것도 이 과정에서 확인했습니다.

**해결 : Facade 패턴으로 트랜잭션 경계 분리**

별도의 `InterviewFacade` 클래스를 두어 AI 호출 구간을 트랜잭션 밖으로 분리했습니다.
Facade는 트랜잭션 없이 메서드 호출 흐름의 제어만 담당하고, Service의 각 메서드가 짧은 트랜잭션 단위로 호출됩니다.

```
InterviewFacade (트랜잭션 없음)
    ├── lockAndSaveAudio()       @Transactional - 짧게 끝남
    ├── AI 서버 호출              트랜잭션 없음 - 커넥션 미점유
    └── saveResultAndUnlock()   @Transactional - 짧게 끝남
```

<br>

**문제 2. 발화가 길수록 STT 대기시간이 선형으로 증가**

말이 끝난 후 전체 음성을 업로드하고 STT를 시작하는 구조였습니다.
짧게 답변하면 빠르고, 길게 답변하면 느린 — 사용자 행동에 따라 응답시간이 들쭉날쭉했습니다.

**해결 : 3초 단위 청크 선행 STT 처리**

말하는 도중 3초 단위로 청크를 WebSocket으로 전송해 STT를 선행 처리합니다.
발화가 끝나는 시점에 STT가 거의 완료된 상태가 되어, **발화 길이에 무관하게 응답시간이 균등**해집니다.

청크 기준은 단순히 정한 게 아니라 VAD(무음 감지)와 고정 시간 기준 두 방식을 직접 PoC했습니다.
1초~10초를 직접 테스트한 결과 3~5초 구간이 정확도와 응답 속도 모두 양호했고, 가장 짧은 3초를 선택했습니다.

> **결과 : STT 대기 시간 2500ms → 1000ms**

<br>

**문제 3. 비동기 청크 응답의 순서 역전**

WebClient로 비동기 STT를 호출하면 3번 청크 응답이 2번보다 먼저 올 수 있습니다.
도착 순서대로 텍스트를 합치면 문장이 뒤섞입니다.

**해결 : Redis Sorted Set으로 순서 보장**

ZSet의 score에 청크 인덱스를 사용하면, 도착 순서와 무관하게 항상 올바른 순서가 보장됩니다.

```
청크 1,2,3 STT 비동기 호출
    → 순서 무관 도착
    → Redis Sorted Set (score = 청크 인덱스)
    → 0:텍스트A  1:텍스트B  2:텍스트C  자동 정렬
    → 조합된 텍스트로 LLM 호출
```

---

### V2 → V3 : Race Condition + 동시성 제어 완성

**문제 1. WebSocket과 HTTP 요청의 Race Condition**

AI 파이프라인(3~5초)이 실행 중일 때 사용자가 종료 버튼을 클릭하면
HTTP 종료 API(0.1초)가 먼저 완료됩니다.
이미 종료된 인터뷰에 꼬리질문이 삽입되는 데이터 무결성 문제가 발생했습니다.

DB 상태 컬럼으로 락을 먼저 구현했지만 두 가지 문제가 있었습니다.
- `@Transactional` 커밋 전 타이밍 문제로 다른 스레드가 변경 전 상태를 읽음
- 서버 비정상 종료 시 `IN_PROGRESS`가 DB에 영구히 남아 해당 인터뷰가 영원히 잠기는 문제

**해결 : Redis TTL 락**

TTL 20초는 정상 파이프라인 최대 완료 시간 약 8초에 여유값을 더한 값입니다.
서버가 비정상 종료되어도 20초 후 자동 해제됩니다.

<br>

**문제 2. 파이프라인 중복 트리거**

다음 버튼 클릭 시 두 경로가 거의 동시에 발생합니다.
- 경로 A : 마지막 청크 STT 완료 (빠름)
- 경로 B : 프론트엔드에서 totalChunks 전송 (느림)

마지막 STT가 먼저 완료되면 Redis에 totalChunks 키가 없어 파이프라인을 트리거할 수 없고,
/next가 나중에 도착해도 재검사 로직이 없으면 파이프라인이 실행되지 않습니다.

**해결 : Redis SETNX로 Exactly-Once 트리거**

두 경로 모두 트리거 조건을 검사하되,
`SETNX`(SET if Not eXists, 원자적 명령어)로 중복 실행을 방지합니다.
두 스레드가 동시에 시도해도 정확히 하나만 성공합니다.

<br>

**문제 3. 종료 파이프라인 순차 실행으로 인한 레이턴시 합산**

STT 정제와 감정 분석은 서로 독립적인 AI 호출임에도 순차 실행되어 레이턴시가 합산됐습니다.

**해결 : Mono.zip()으로 병렬 실행**

```
순차 실행 : STT 정제(T1) → 감정 분석(T2)  →  레이턴시: T1 + T2
병렬 실행 : STT 정제(T1)                  →  레이턴시: max(T1, T2)
           감정 분석(T2) ↗
```

키워드 저장, RAG 적재처럼 사용자 응답과 무관한 작업은
`doOnSuccess() + subscribe()`로 메인 체인에서 분리해 응답을 블로킹하지 않도록 했습니다.

> **결과 : 전체 응답 시간 8~10초 → 4초 (BE 단독 기여)**

---

## 기억에 남는 디버깅 — Tomcat WebSocket 1009

React에서만 WebSocket 연결이 Status 1009로 끊기고 세션이 중복 생성되는 현상이 있었습니다.
바닐라 JS로 동일한 테스트를 하면 정상 동작했습니다.

서버 로그를 두 환경에서 비교했습니다.

| 환경 | 로그 패턴 |
|---|---|
| 바닐라 JS (sockjs-client) | 16KB씩 분할 전송, Spring이 메모리에서 조립 후 컨트롤러 전달 |
| React (@stomp/stompjs) | 수백 KB를 단일 프레임으로 전송, Tomcat 레벨에서 즉시 1009 반환 |

원인은 **라이브러리별 전송 방식 차이**였습니다.
`@stomp/stompjs`가 보낸 단일 프레임이 Tomcat 기본 버퍼(8KB)를 초과하면
STOMP 핸들러에 도달하기도 전에 Tomcat이 연결을 끊습니다.

Spring WebSocket에는 버퍼 설정 레이어가 두 개 존재합니다.

```
클라이언트
    ↓
Tomcat Container 레벨   ← 여기가 문제 (기본 8KB)
    ↓
Spring Transport 레벨   ← 이것만 설정되어 있었음
    ↓
@MessageMapping 컨트롤러
```

`ServletServerContainerFactoryBean`으로 Tomcat Container 레벨의 버퍼 용량을 늘려 해결했습니다.

---

## 시스템 아키텍처

<img width="12452" height="11201" alt="readme_system_architecture" src="https://github.com/user-attachments/assets/7c34778b-9e62-4621-b3b7-cc3afe167868" />

---

## ERD

<img width="1192" height="801" alt="readme_erd_1" src="https://github.com/user-attachments/assets/cffe1362-5e60-4d88-b434-968ea9b4e7d4" />
