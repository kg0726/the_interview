package com.theinterview.domain.interview.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.theinterview.domain.interview.dto.InterviewRequest.InterviewExitRequest;
import com.theinterview.domain.interview.dto.InterviewRequest.InterviewStartRequest;
import com.theinterview.domain.interview.dto.InterviewRequest.TotalChunkRequest;
import com.theinterview.domain.interview.dto.InterviewResponse.WebSocketConnectResponse;
import com.theinterview.domain.interview.facade.InterviewFacade;
import com.theinterview.domain.interview.service.InterviewService;
import com.theinterview.global.config.redis.RedisService;
import com.theinterview.global.response.ApiResponse;
import com.theinterview.global.response.exception.GeneralException;
import com.theinterview.global.response.status.ErrorStatus;
import java.io.IOException;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.User;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/*
 * ===========================================================================
 *  InterviewController — HTTP 진입점 발췌 (인터뷰 시작 + V2 종료 API 만)
 * ===========================================================================
 *
 *  포트폴리오 핵심 — exitV2 가 두 가지 race condition 을 어떻게 막는지
 *  ----------------------------------------------------------------------
 *
 *  Race 1: WebSocket AI 파이프라인(3~5s) 실행 중에 HTTP 종료(0.1s) 가 먼저 끝남
 *    → "interviewId:{id}lock" Redis TTL 락 으로 차단.
 *      한쪽이 락을 들고 있으면 409 로 거부. TTL 20s 로 서버 비정상 종료에도 자동 해제.
 *
 *  Race 2: WebSocket 마지막 청크와 HTTP /exitV2 가 거의 동시에 도착
 *    → SETNX("...triggered") 로 트리거 점유. 정확히 한 경로만 파이프라인 실행.
 *      WebSocketController 의 청크 경로 / /next 경로와 같은 키를 공유하므로
 *      세 경로 모두 동시에 도착해도 안전.
 *
 *  ※ getInterviewsHistory / getInterviewHistoryDetail / deleteInterview / share / bookmark /
 *    explore / serveVoice / restart / finish 등 CRUD 17 개 핸들러는 발췌에서 제외.
 * ===========================================================================
 */
@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/v1/interview")
public class InterviewController {

    private final InterviewService interviewService;
    private final RedisService redisService;
    private final InterviewFacade interviewFacade;

    /**
     * 인터뷰 시작 — 첫 질문 TTS 가 준비된 뒤 WebSocket 연결을 위한 interviewId / questionId 발급.
     * <p>
     * Facade 가 Mono 를 반환하지만 컨트롤러는 톰캣 스레드 모델이므로 .block() 한 번만 사용.
     */
    @PostMapping
    public ApiResponse<WebSocketConnectResponse> readForConnectWebSocket(
            @RequestBody InterviewStartRequest request,
            @AuthenticationPrincipal User user
    ) throws JsonProcessingException {
        String email = user.getUsername();
        return ApiResponse.onSuccess(
                interviewFacade.issueWebSocketId(email, request.getVoice(), false, null)
                        .block());
    }

    /**
     * 인터뷰 종료 V2 — 사용자가 종료 버튼을 눌렀을 때.
     * <p>
     * 핵심 책임:
     *   1. Redis TTL 락 으로 진행 중인 WebSocket 파이프라인과의 동시 실행 차단 (Race 1)
     *   2. isExit / isSaving 플래그를 Redis 에 기록 → WebSocket 컨트롤러가 분기에 활용
     *   3. 마지막 청크가 이미 STT 완료된 상태라면 SETNX 로 점유 후 직접 종료 파이프라인 호출 (Race 2)
     *      그렇지 않으면 컨트롤러는 200 OK 만 반환하고, WebSocket 마지막 청크 STT 완료 시점에
     *      receiveChunkAudio 가 isExit 플래그를 보고 종료 분기로 진입한다.
     */
    @PostMapping("/exitV2")
    public ApiResponse<Void> exitInterviewV2(
            @RequestBody TotalChunkRequest request,
            @AuthenticationPrincipal User user
    ) throws IOException {

        long controllerStart = System.currentTimeMillis();
        String email = user.getUsername();
        log.info("[EXIT_TIMING] exitV2 요청 수신 — interviewId: {}, totalChunk: {}",
                request.getInterviewId(), request.getTotalChunk());

        // === Race 1 차단: WebSocket 파이프라인이 진행 중이면 종료 거부 ===
        if (redisService.hasKey("interviewId:" + request.getInterviewId() + "lock")) {
            throw new GeneralException(ErrorStatus.QUESTION_NOT_SAVED);
        }

        String redisKey = "interviewId:" + request.getInterviewId()
                + "questionId:" + request.getQuestionId();

        // WebSocket 컨트롤러가 분기 시 참조할 종료 신호 + 저장 진행 중 플래그
        redisService.setValues("interviewId:" + request.getInterviewId() + "isExit",
                "true", Duration.ofSeconds(20));
        redisService.setValues("interviewId:" + request.getInterviewId() + "isSaving",
                "inProgress", Duration.ofSeconds(30));

        // 사용자가 녹음을 시작하기도 전에 종료한 케이스 — 짧은 종료 흐름으로 분기
        if (request.getAnswerAudio() == null || request.getAnswerAudio().length == 0) {
            redisService.setValues("interviewId:" + request.getInterviewId() + "lock", "lock",
                    Duration.ofSeconds(20));
            interviewFacade.exitInterviewNoAudioFacade(/* request, email, "interviewId:" + ... */);
            redisService.deleteValues("interviewId:" + request.getInterviewId() + "lock");
            return ApiResponse.OK;
        }

        // 정상 종료 — 음성 파일과 totalChunks 를 Redis 에 기록
        String savedPath = interviewService.saveAnswerFile(request.getAnswerAudio(),
                request.getInterviewId());
        interviewService.saveRequiredFlagInRedis(savedPath, redisKey, request.getTotalChunk());

        /*
         * === Race 2: SETNX 트리거 ===
         * exitV2 도착 시점에 이미 모든 청크 STT 가 끝나 있다면 여기서 직접 트리거한다.
         * 그렇지 않으면 마지막 청크 STT 가 완료된 시점에 WebSocket 컨트롤러가 트리거한다.
         * 어느 쪽이 SETNX 에 성공하든 파이프라인은 한 번만 실행됨.
         */
        int totalChunks = Integer.parseInt(request.getTotalChunk());
        long savedChunkCount = redisService.savedChunkCount(redisKey + "chunks");

        if (totalChunks == savedChunkCount) {
            String triggerKey = redisKey + "triggered";
            if (redisService.setIfAbsent(triggerKey, "1", Duration.ofMinutes(5))) {
                log.info("[EXIT_TIMING] exitV2 경로에서 파이프라인 직접 트리거 — interviewId: {}",
                        request.getInterviewId());
                interviewFacade.exitInterviewFacade(email,
                                InterviewExitRequest.builder()
                                        .interviewId(request.getInterviewId())
                                        .questionId(request.getQuestionId())
                                        .build())
                        .subscribe(
                                null,
                                error -> {
                                    log.error("종료 파이프라인 에러 (exitV2 경로) interviewId: {}, 원인: {}",
                                            request.getInterviewId(), error.getMessage());
                                    cleanupOnError(redisKey);
                                },
                                () -> {
                                    redisService.deleteValues(redisKey + "triggered");
                                    log.info("종료 파이프라인 완료 (exitV2 경로) — interviewId: {}",
                                            request.getInterviewId());
                                }
                        );
            }
        }

        log.info("[EXIT_TIMING] exitV2 컨트롤러 반환 — interviewId: {}, 컨트롤러 총 소요: {}ms",
                request.getInterviewId(), System.currentTimeMillis() - controllerStart);

        return ApiResponse.OK;
    }

    private void cleanupOnError(String redisKey) {
        redisService.deleteValues(redisKey + "triggered");
        redisService.deleteValues(redisKey + "chunks");
        redisService.deleteValues(redisKey + "totalChunks");
        redisService.deleteValues(redisKey + "savedPath");
    }

    // ===========================================================================
    //  생략된 핸들러 (포트폴리오 핵심과 무관)
    //
    //   - exitInterview (V1 동기 종료, V2 로 대체됨)
    //   - getInterviewsHistory / getInterviewHistoryDetail / deleteInterview
    //   - setInterviewShareRange / getInterviewShareLink / getSharedInterviewDetail
    //   - getSharedPostItBoard / getInterviewExplore / setBookmark / serveVoice
    //   - findUnfinishedInterviews / restartInterview / finishInterview
    // ===========================================================================
}
