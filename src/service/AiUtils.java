package com.theinterview.domain.interview.service;

import com.theinterview.domain.interview.dto.AiRagDto.DeleteRequest;
import com.theinterview.domain.interview.dto.AiRagDto.DeleteResponse;
import com.theinterview.domain.interview.dto.AiRagDto.UpsertRequest;
import com.theinterview.domain.interview.dto.AiRagDto.UpsertResponse;
import com.theinterview.domain.interview.dto.FastApiRequest.AiNextOpeningRequest;
import com.theinterview.domain.interview.dto.FastApiRequest.AiStartTtsRequest;
import com.theinterview.domain.interview.dto.FastApiRequest.AiTitleRequest;
import com.theinterview.domain.interview.dto.FastApiRequest.KeywordRequest;
import com.theinterview.domain.interview.dto.FastApiRequest.LastAnswerRequest;
import com.theinterview.domain.interview.dto.FastApiResponse.AiNextOpeningResponse;
import com.theinterview.domain.interview.dto.FastApiResponse.AiPipelineResponse;
import com.theinterview.domain.interview.dto.FastApiResponse.AiStartTtsResponse;
import com.theinterview.domain.interview.dto.FastApiResponse.AiTitleResponse;
import com.theinterview.domain.interview.dto.FastApiResponse.KeywordResponse;
import com.theinterview.domain.interview.dto.FastApiResponse.LastAnswerEmotionResponse;
import com.theinterview.domain.interview.dto.FastApiResponse.LastAnswerTextResponse;
import com.theinterview.domain.interview.dto.InterviewRequest.AudioChunkRequest;
import com.theinterview.domain.interview.dto.InterviewResponse.SttChunkResponse;
import com.theinterview.global.response.exception.GeneralException;
import com.theinterview.global.response.status.ErrorStatus;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/*
 * ===========================================================================
 *  AiUtils — FastAPI(AI 서버) 호출을 모두 모은 WebClient 어댑터
 * ===========================================================================
 *
 *  설계 원칙
 *  ----------------------------------------------------------------------
 *  1. V3 메서드는 모두 Mono<...> 를 반환한다. .block() 을 호출하지 않는다.
 *     → Facade 레이어가 Mono.zip / flatMap 으로 자유롭게 병렬화 가능.
 *
 *  2. 4xx/5xx 를 onStatus 로 분기 처리한다.
 *     - 4xx: IllegalArgumentException 으로 던져서 retryWhen 의 .filter 에서 재시도 제외
 *     - 5xx: GeneralException 으로 정상 재시도 흐름에 태움
 *     이렇게 하면 멱등성이 보장되지 않는 4xx 요청을 무의미하게 다시 보내는 비용을 피할 수 있다.
 *
 *  3. RAG 호출은 fixed delay 재시도 정책을 공유 (getRagRetrySpec).
 *
 *  ※ V1 의 .block() 동기 메서드 (callFastApiForNextQuestion / callFastApiForInterviewTitle /
 *    callFastApiForNextOpening) 는 V2 가 출현하면서 사용 빈도가 크게 줄었으므로 발췌에서 제외.
 *  ※ callFastApiForRagReindex 는 callFastApiForRagUpsert 와 거의 동일한 패턴이므로 한 개만 노출.
 * ===========================================================================
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AiUtils {

    private final WebClient webClient;

    // ===========================================================================
    //  STT 청크 — V2 핵심
    // ===========================================================================

    /**
     * 클라이언트가 3초 단위로 잘라 보내는 음성 청크 한 개를 STT 로 변환한다.
     * <p>
     * 비동기로 발사되므로 청크 1, 2, 3 의 응답이 도착하는 순서가 뒤섞일 수 있다.
     * → 호출자(InterviewService.chunkAudioSTT) 가 결과를 Redis Sorted Set 에
     *   score=chunkIndex 로 적재해 발화 순서를 자동 정렬한다.
     */
    public Mono<SttChunkResponse> callFastApiForSttChunk(AudioChunkRequest request) {

        MultipartBodyBuilder bodyBuilder = new MultipartBodyBuilder();

        ByteArrayResource audioResource = new ByteArrayResource(request.getAudioData()) {
            @Override
            public String getFilename() {
                return "answer.webm";
            }
        };
        bodyBuilder.part("audio_file", audioResource)
                .contentType(MediaType.parseMediaType("audio/webm"));

        log.info("청킹된 오디오 파일 STT 변환을 위한 통신 시작");

        return webClient.post()
                .uri("api/v1/ai/stt")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(bodyBuilder.build()))
                .retrieve()
                // 비동기 환경에서는 try-catch 가 작동하지 않음 → 체인 내부에서 Mono.error 로 신호
                .onStatus(status -> status.is4xxClientError(), clientResponse -> {
                    log.error("FastAPI 4xx 에러 발생");
                    return Mono.error(new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR));
                })
                .onStatus(status -> status.is5xxServerError(), clientResponse -> {
                    log.error("FastAPI 5xx 에러 발생");
                    return Mono.error(new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR));
                })
                .bodyToMono(SttChunkResponse.class);
    }

    // ===========================================================================
    //  꼬리질문 생성 — V3 비동기
    // ===========================================================================

    /**
     * 청크 STT 가 완료된 시점에 호출되는 메인 파이프라인.
     * <p>
     * V1 의 동기 동명 메서드는 .block() 으로 톰캣 스레드를 점유했으나, 이 V2 는 Mono 를 반환해
     * Facade 의 reactive 체인에 그대로 합성된다. 결과적으로 톰캣 워커 스레드를 한 번도
     * 블로킹하지 않고 AI 응답을 기다릴 수 있게 된다.
     */
    public Mono<AiPipelineResponse> callFastApiForNextQuestionV2(
            byte[] audioBytes, Integer interviewId, Integer questionId,
            int sequence, String nickname, String previousQuestion,
            Boolean isFirstFinished, String sttText, String voice) {

        MultipartBodyBuilder bodyBuilder = new MultipartBodyBuilder();

        ByteArrayResource audioResource = new ByteArrayResource(audioBytes) {
            @Override
            public String getFilename() {
                return "answer.webm";
            }
        };
        bodyBuilder.part("audio_file", audioResource)
                .contentType(MediaType.parseMediaType("audio/webm"));

        bodyBuilder.part("is_first_finished", isFirstFinished);
        bodyBuilder.part("sequence", sequence);
        bodyBuilder.part("nickname", nickname);
        if (sequence >= 2 && previousQuestion != null) {
            bodyBuilder.part("previous_question", previousQuestion);
        }

        bodyBuilder.part("stt_text", sttText);
        /*
         * AI 서버가 정제 텍스트를 다시 백엔드로 콜백할 때 현재 답변 대상을 식별할 수 있어야 하므로
         * after-stt 요청 자체에 interview_id, question_id 를 함께 넘긴다. AI 서버가 별도 상태 추적
         * 없이 그대로 콜백 payload 를 구성할 수 있게 하기 위함.
         */
        bodyBuilder.part("interview_id", interviewId);
        bodyBuilder.part("question_id", questionId);
        if (voice != null && !voice.isBlank()) {
            bodyBuilder.part("voice", voice);
        }

        return webClient.post()
                .uri("/api/v1/ai/interview/pipeline/after-stt")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(bodyBuilder.build()))
                .retrieve()
                .onStatus(status -> status.is4xxClientError(), clientResponse -> {
                    log.error("FastAPI 클라이언트 에러 (4xx): {}", clientResponse.statusCode());
                    return Mono.error(new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR));
                })
                .onStatus(status -> status.is5xxServerError(), clientResponse -> {
                    log.error("FastAPI 서버 에러 (5xx): {}", clientResponse.statusCode());
                    return Mono.error(new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR));
                })
                .bodyToMono(AiPipelineResponse.class);
    }

    // ===========================================================================
    //  종료 흐름 — Mono.zip 으로 병렬화될 메서드들
    // ===========================================================================

    /**
     * 종료 시 마지막 답변에 대한 STT 정제 호출.
     * Facade 에서 lastAnswerEmotion 과 Mono.zip 으로 병렬 실행됨.
     */
    public Mono<LastAnswerTextResponse> lastAnswerStt(String answerText) {
        return webClient.post()
                .uri("/api/v1/ai/interview/refine-answer")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(LastAnswerRequest.builder().sttText(answerText).build())
                .retrieve()
                .onStatus(status -> status.is4xxClientError(), clientResponse -> {
                    log.error("4xx 클라이언트 에러: {}", clientResponse.statusCode());
                    return Mono.error(new IllegalArgumentException("4xx Client Error"));
                })
                .onStatus(status -> status.is5xxServerError(), clientResponse -> {
                    log.error("5xx 서버 에러: {}", clientResponse.statusCode());
                    return Mono.error(new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR));
                })
                .bodyToMono(LastAnswerTextResponse.class);
    }

    /**
     * 종료 시 마지막 답변에 대한 감정 분석 호출(텍스트 + 음성 동시 입력).
     * lastAnswerStt 와 Mono.zip 으로 병렬 실행됨.
     */
    public Mono<LastAnswerEmotionResponse> lastAnswerEmotion(String answerText, byte[] audio) {
        MultipartBodyBuilder builder = new MultipartBodyBuilder();
        builder.part("stt_text", answerText);
        builder.part("audio_file", audio)
                .header("Content-Disposition", "form-data; name=audio_file; filename=audio.webm")
                .header("Content-Type", "audio/webm");

        return webClient.post()
                .uri("/api/v1/ai/interview/state-analysis")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .bodyValue(builder.build())
                .retrieve()
                .onStatus(status -> status.is4xxClientError(), clientResponse -> {
                    log.error("4xx 클라이언트 에러: {}", clientResponse.statusCode());
                    return Mono.error(new IllegalArgumentException("4xx Client Error"));
                })
                .onStatus(status -> status.is5xxServerError(), clientResponse -> {
                    log.error("5xx 서버 에러: {}", clientResponse.statusCode());
                    return Mono.error(new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR));
                })
                .bodyToMono(LastAnswerEmotionResponse.class);
    }

    /** 종료 시 인터뷰 제목 생성 — callFastApiForNextOpeningV2 와 Mono.zip 으로 병렬 실행됨. */
    public Mono<AiTitleResponse> callFastApiForInterviewTitleV2(AiTitleRequest request) {
        return webClient.post()
                .uri("/api/v1/ai/interview/title")
                .bodyValue(request)
                .retrieve()
                .onStatus(status -> status.is4xxClientError(), clientResponse -> {
                    log.error("4xx 클라이언트 에러: {}", clientResponse.statusCode());
                    return Mono.error(new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR));
                })
                .onStatus(status -> status.is5xxServerError(), clientResponse -> {
                    log.error("5xx 서버 에러: {}", clientResponse.statusCode());
                    return Mono.error(new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR));
                })
                .bodyToMono(AiTitleResponse.class);
    }

    /** 종료 시 다음 회차의 첫 질문(오프닝) 생성 — callFastApiForInterviewTitleV2 와 병렬 실행됨. */
    public Mono<AiNextOpeningResponse> callFastApiForNextOpeningV2(AiNextOpeningRequest request) {
        return webClient.post()
                .uri("/api/v1/ai/interview/next-opening")
                .bodyValue(request)
                .retrieve()
                .onStatus(status -> status.is4xxClientError(), clientResponse -> {
                    log.error("4xx 클라이언트 에러: {}", clientResponse.statusCode());
                    return Mono.error(new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR));
                })
                .onStatus(status -> status.is5xxServerError(), clientResponse -> {
                    log.error("5xx 서버 에러: {}", clientResponse.statusCode());
                    return Mono.error(new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR));
                })
                .bodyToMono(AiNextOpeningResponse.class);
    }

    // ===========================================================================
    //  메인 응답과 분리되는 호출들 (doOnSuccess + 별도 subscribe 로 fire-and-forget)
    // ===========================================================================

    /** 키워드 추출 — 메인 응답을 막지 않도록 doOnSuccess 안에서 별도 subscribe 됨. */
    public Mono<KeywordResponse> callFastApiKeyword(KeywordRequest request) {
        return webClient.post()
                .uri("api/v1/ai/interview/keyword")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .onStatus(status -> status.is4xxClientError(), clientResponse -> {
                    log.error("4xx 클라이언트 에러: {}", clientResponse.statusCode());
                    // IllegalArgumentException → retryWhen.filter 에서 재시도 제외 신호
                    return Mono.error(new IllegalArgumentException("4xx Client Error"));
                })
                .onStatus(status -> status.is5xxServerError(), clientResponse -> {
                    log.error("5xx 서버 에러: {}", clientResponse.statusCode());
                    return Mono.error(new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR));
                })
                .bodyToMono(KeywordResponse.class);
    }

    /** RAG 적재 — 종료 시 doOnSuccess 안에서 별도 subscribe 됨. 실패해도 사용자 화면 전환에 영향 없음. */
    public Mono<UpsertResponse> callFastApiForRagUpsert(UpsertRequest request) {
        return webClient.post()
                .uri("/api/v1/ai/interview/rag/upsert")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .onStatus(status -> status.is4xxClientError(), clientResponse -> {
                    log.error("RAG Upsert 4xx 에러: {}", clientResponse.statusCode());
                    return Mono.error(new IllegalArgumentException("4xx Client Error"));
                })
                .onStatus(status -> status.is5xxServerError(), clientResponse -> {
                    log.error("RAG Upsert 5xx 에러: {}", clientResponse.statusCode());
                    return Mono.error(new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR));
                })
                .bodyToMono(UpsertResponse.class)
                .retryWhen(getRagRetrySpec());
    }

    /** 인터뷰 삭제 시 RAG 정리 — 같은 retry 정책 적용. */
    public Mono<DeleteResponse> callFastApiForRagDelete(DeleteRequest request) {
        return webClient.method(HttpMethod.DELETE)
                .uri("/api/v1/ai/interview/rag/delete")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .onStatus(status -> status.is4xxClientError(), clientResponse -> {
                    log.error("RAG Delete 4xx 에러: {}", clientResponse.statusCode());
                    return Mono.error(new IllegalArgumentException("4xx Client Error"));
                })
                .onStatus(status -> status.is5xxServerError(), clientResponse -> {
                    log.error("RAG Delete 5xx 에러: {}", clientResponse.statusCode());
                    return Mono.error(new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR));
                })
                .bodyToMono(DeleteResponse.class)
                .retryWhen(getRagRetrySpec());
    }

    // ===========================================================================
    //  인터뷰 시작 TTS — 첫 질문 생성
    // ===========================================================================

    /** 첫 질문에 대한 TTS 생성. WebSocket 연결 직전에 한 번 호출. */
    public Mono<AiStartTtsResponse> callFastApiForStartTts(AiStartTtsRequest request) {
        return webClient.post()
                .uri("/api/v1/ai/interview/start-tts")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .bodyToMono(AiStartTtsResponse.class)
                .doOnError(e -> log.error("TTS 통신 실패: {}", e.getMessage()));
    }

    // ===========================================================================
    //  공용 재시도 정책
    // ===========================================================================

    /**
     * RAG 호출 실패 시 재시도 정책 — 2초 간격, 최대 3회.
     * IllegalArgumentException(=4xx) 은 멱등성이 없으므로 재시도하지 않음.
     */
    private Retry getRagRetrySpec() {
        return Retry.fixedDelay(3, Duration.ofSeconds(2))
                .filter(throwable -> {
                    if (throwable instanceof IllegalArgumentException) {
                        log.warn("RAG 4xx 에러 — 멱등성이 없으므로 재시도 생략");
                        return false;
                    }
                    log.warn("RAG 5xx 또는 네트워크 단절 — 재시도");
                    return true;
                });
    }

    // ===========================================================================
    //  생략된 메서드 (참고)
    //
    //   - callFastApiForNextQuestion (V1 동기, .block() 사용) — V2 로 대체됨
    //   - callFastApiForInterviewTitle / callFastApiForNextOpening (V1 동기) — V2 로 대체됨
    //   - callFastApiForRagReindex — callFastApiForRagUpsert 와 패턴이 동일
    // ===========================================================================
}
