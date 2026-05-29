package com.theinterview.domain.interview.controller;

import com.theinterview.domain.interview.dto.InterviewRequest.AudioChunkRequest;
import com.theinterview.domain.interview.dto.InterviewRequest.AudioMessageRequest;
import com.theinterview.domain.interview.dto.InterviewRequest.InterviewExitRequest;
import com.theinterview.domain.interview.dto.InterviewRequest.TotalChunkRequest;
import com.theinterview.domain.interview.dto.InterviewResponse.FollowUpQuestionDTO;
import com.theinterview.domain.interview.facade.InterviewFacade;
import com.theinterview.domain.interview.service.InterviewService;
import com.theinterview.global.config.redis.RedisService;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Mono;

/*
 * ===========================================================================
 *  InterviewWebSocketController — STOMP 청크 수신 + Exactly-Once 트리거
 * ===========================================================================
 *
 *  핵심 문제 — "다음 버튼을 누르면 두 이벤트가 거의 동시에 발생한다"
 *  ----------------------------------------------------------------------
 *    경로 A: 마지막 청크 STT 완료 (빠름)
 *    경로 B: 프론트가 fullRecorder.stop() 후 /next 로 totalChunks 전송 (느림)
 *
 *  완료 조건은 "ZSet.size == totalChunks". 두 경로 모두 이 조건을 검사할 수 있어야 한다
 *  (어느 쪽이 먼저 도착할지 모르므로). 하지만 파이프라인은 정확히 한 번만 트리거되어야 한다.
 *
 *  해결
 *  ----------------------------------------------------------------------
 *    - 두 경로 모두 완료 조건 검사 후 Redis SETNX("...triggered") 로 원자적 점유 시도
 *    - SETNX 가 true 를 반환한 쪽만 파이프라인 실행, 진 쪽은 그냥 무시
 *    - 파이프라인 완료/실패 시 triggered 키 삭제 → 같은 인터뷰의 다음 답변 재진입 보장
 *
 *  파이프라인 실패 시 onError 핸들러
 *  ----------------------------------------------------------------------
 *    네트워크 단절이나 AI 5xx 등으로 파이프라인이 중간에 깨졌을 때, Redis 의 chunks/totalChunks/
 *    savedPath/triggered/lock 키와 saveAnswer 가 만든 고아 InterviewAnswer 행을 모두 정리해
 *    사용자가 같은 질문을 다시 시도할 수 있도록 한다.
 * ===========================================================================
 */
@Slf4j
@Controller
@RequiredArgsConstructor
public class InterviewWebSocketController {

    private final SimpMessagingTemplate messagingTemplate;
    private final InterviewService interviewService;
    private final InterviewFacade interviewFacade;
    private final RedisService redisService;

    /**
     * V1 — 전체 음성을 한 번에 전송하는 동기 엔드포인트. V2/V3 청크 흐름과의 비교를 위해 남겨둠.
     */
    @MessageMapping("/interview/audio")
    public void receiveAudio(@Payload AudioMessageRequest request) throws IOException {
        long start = System.currentTimeMillis();
        FollowUpQuestionDTO response = interviewFacade.processOneQnA(request);
        log.info("요청 및 반환 종료. 걸린 시간: {}", System.currentTimeMillis() - start);
        messagingTemplate.convertAndSend("/topic/interview/room/" + request.getInterviewId(), response);
    }

    /**
     * V2/V3 — 청크된 오디오 조각이 도착하는 메인 경로 (경로 A).
     * <p>
     * 1) chunkAudioSTT → STT 비동기 호출 + Sorted Set 적재
     * 2) 모든 청크 수신 완료 시 SETNX 로 트리거 점유 시도
     * 3) 종료/꼬리질문 분기 후 Facade 호출
     */
    @MessageMapping("interview/audio/chunk")
    public void receiveChunkAudio(
            @Payload AudioChunkRequest request,
            @Header("simpSessionAttributes") Map<String, Object> sessionAttributes
    ) {

        String email = (String) sessionAttributes.get("email");
        long start = System.currentTimeMillis();

        interviewService.chunkAudioSTT(request)
                .flatMap(isLastChunk -> {
                    if (!isLastChunk) {
                        return Mono.empty();
                    }

                    log.info("모든 청크 조각 도착 완료");

                    // === 핵심: SETNX 로 원자적 트리거 점유 ===
                    String triggerKey = "interviewId:" + request.getInterviewId()
                            + "questionId:" + request.getQuestionId() + "triggered";
                    if (!redisService.setIfAbsent(triggerKey, "1", Duration.ofMinutes(5))) {
                        log.info("파이프라인 이미 트리거됨 — 청크 경로 중복 실행 방지");
                        return Mono.empty();
                    }

                    // 사용자가 종료를 눌렀는지(isExit) vs 다음을 눌렀는지 분기
                    if (redisService.hasKey("interviewId:" + request.getInterviewId() + "isExit")) {
                        return interviewFacade.exitInterviewFacade(email,
                                InterviewExitRequest.builder()
                                        .interviewId(request.getInterviewId())
                                        .questionId(request.getQuestionId())
                                        .build());
                    }

                    return interviewFacade.generateNextQuestionFacade(
                                    request.getInterviewId(), request.getQuestionId(), start, email)
                            .doOnNext(followUpQuestionDTO ->
                                    messagingTemplate.convertAndSend(
                                            "/topic/interview/tmp/" + request.getInterviewId(),
                                            followUpQuestionDTO));
                })
                .subscribe(
                        null,
                        // 파이프라인 실패 시 — Redis 상태 정리 + 고아 답변 행 제거
                        error -> {
                            log.error("청크 STT 파이프라인 에러 interviewId: {}, 원인: {}",
                                    request.getInterviewId(), error.getMessage());
                            cleanupOnError(request.getInterviewId(), request.getQuestionId());
                            interviewService.deleteAnswerByQuestionId(request.getQuestionId());
                            messagingTemplate.convertAndSend(
                                    "/topic/interview/tmp/" + request.getInterviewId(),
                                    (Object) Map.of("type", "STT_ERROR", "message", error.getMessage()));
                        },
                        // 성공 완료 — triggered 키만 삭제해 다음 답변 재진입 허용
                        () -> {
                            String redisKeyPrefix = "interviewId:" + request.getInterviewId()
                                    + "questionId:" + request.getQuestionId();
                            redisService.deleteValues(redisKeyPrefix + "triggered");
                            log.info("파이프라인 완료 — triggered 키 삭제 interviewId: {}", request.getInterviewId());
                        }
                );
    }

    /**
     * V2/V3 — /next 이벤트 (경로 B). totalChunks 와 음성 파일 저장 경로를 Redis 에 기록한다.
     * <p>
     * 도착 시점에 이미 모든 청크 STT 가 완료된 상태라면 — 즉 경로 A 보다 늦게 도착한 경우 —
     * 이 경로에서도 SETNX 로 트리거를 시도한다. 동일 키이므로 경로 A 가 이미 점유했다면 자연스럽게 무시된다.
     */
    @MessageMapping("/next")
    public void receiveTotalChunk(
            @Payload TotalChunkRequest request,
            @Header("simpSessionAttributes") Map<String, Object> sessionAttributes
    ) throws IOException {

        String email = (String) sessionAttributes.get("email");
        String redisKey = "interviewId:" + request.getInterviewId()
                + "questionId:" + request.getQuestionId();

        // === Redis TTL 락 — HTTP 종료 API(/exitV2) 와의 동시 진입을 막는다 ===
        // TTL 20s = 정상 파이프라인 최대 ~8s + 여유. 서버 비정상 종료 시에도 자동 해제됨.
        redisService.setValues("interviewId:" + request.getInterviewId() + "lock", "lock",
                Duration.ofSeconds(20));

        String savedPath = interviewService.saveAnswerFile(request.getAnswerAudio(),
                request.getInterviewId());
        redisService.setValues(redisKey + "totalChunks", request.getTotalChunk(), Duration.ofMinutes(3));
        redisService.setValues(redisKey + "savedPath", savedPath, Duration.ofMinutes(3));

        // 경로 B 도 완료 조건 검사 — 내가 늦게 도착한 케이스 처리
        int totalChunks = Integer.parseInt(request.getTotalChunk());
        if (totalChunks != redisService.savedChunkCount(redisKey + "chunks")) {
            return; // 아직 STT 미완료 — 경로 A 가 마무리 시점에 트리거할 것
        }

        String triggerKey = redisKey + "triggered";
        if (!redisService.setIfAbsent(triggerKey, "1", Duration.ofMinutes(5))) {
            return; // 경로 A 가 이미 점유 — 이쪽은 그냥 종료
        }

        log.info("totalChunks 도착 시점에 모든 청크 완료 확인 — 파이프라인 트리거 (/next 경로)");

        if (redisService.hasKey("interviewId:" + request.getInterviewId() + "isExit")) {
            interviewFacade.exitInterviewFacade(email, InterviewExitRequest.builder()
                            .interviewId(request.getInterviewId())
                            .questionId(request.getQuestionId())
                            .build())
                    .subscribe(null,
                            error -> handleNextPathError("/next", request, error),
                            () -> {
                                redisService.deleteValues(redisKey + "triggered");
                                log.info("종료 파이프라인 완료 — triggered 키 삭제 (/next 경로)");
                            });
        } else {
            interviewFacade.generateNextQuestionFacade(
                            request.getInterviewId(), request.getQuestionId(),
                            System.currentTimeMillis(), email)
                    .doOnNext(followUpQuestionDTO ->
                            messagingTemplate.convertAndSend(
                                    "/topic/interview/tmp/" + request.getInterviewId(),
                                    followUpQuestionDTO))
                    .subscribe(null,
                            error -> handleNextPathError("/next", request, error),
                            () -> {
                                redisService.deleteValues(redisKey + "triggered");
                                log.info("꼬리질문 파이프라인 완료 — triggered 키 삭제 (/next 경로)");
                            });
        }
    }

    // ----- 에러 정리 -----

    private void cleanupOnError(Integer interviewId, Integer questionId) {
        String prefix = "interviewId:" + interviewId + "questionId:" + questionId;
        redisService.deleteValues(prefix + "triggered");
        redisService.deleteValues(prefix + "chunks");
        redisService.deleteValues(prefix + "totalChunks");
        redisService.deleteValues(prefix + "savedPath");
        redisService.deleteValues("interviewId:" + interviewId + "lock");
    }

    private void handleNextPathError(String path, TotalChunkRequest request, Throwable error) {
        log.error("파이프라인 에러 ({} 경로) interviewId: {}, 원인: {}",
                path, request.getInterviewId(), error.getMessage());
        cleanupOnError(request.getInterviewId(), request.getQuestionId());
        interviewService.deleteAnswerByQuestionId(request.getQuestionId());
        messagingTemplate.convertAndSend(
                "/topic/interview/tmp/" + request.getInterviewId(),
                (Object) Map.of("type", "STT_ERROR", "message", error.getMessage()));
    }
}
