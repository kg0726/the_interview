package com.theinterview.domain.interview.facade;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.theinterview.domain.interview.Repository.InterviewQuestionRepository;
import com.theinterview.domain.interview.dto.AiRagDto.UpsertRequest;
import com.theinterview.domain.interview.dto.AiRagDto.Turn;
import com.theinterview.domain.interview.dto.FastApiRequest.AiNextOpeningRequest;
import com.theinterview.domain.interview.dto.FastApiRequest.AiPipelineReadyDto;
import com.theinterview.domain.interview.dto.FastApiRequest.AiTitleRequest;
import com.theinterview.domain.interview.dto.FastApiRequest.KeywordRequest;
import com.theinterview.domain.interview.dto.FastApiResponse.AiNextOpeningResponse;
import com.theinterview.domain.interview.dto.FastApiResponse.AiTitleResponse;
import com.theinterview.domain.interview.dto.InterviewRequest.InterviewExitRequest;
import com.theinterview.domain.interview.dto.InterviewResponse.FollowUpQuestionDTO;
import com.theinterview.domain.interview.entity.InterviewQuestion;
import com.theinterview.domain.interview.service.AiUtils;
import com.theinterview.domain.interview.service.InterviewService;
import com.theinterview.domain.member.Member;
import com.theinterview.global.config.redis.RedisService;
import com.theinterview.global.response.exception.GeneralException;
import com.theinterview.global.response.status.ErrorStatus;
import com.theinterview.global.util.DomainFindUtils;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/*
 * ===========================================================================
 *  InterviewFacade — 인터뷰 파이프라인 오케스트레이션 레이어
 * ===========================================================================
 *
 *  V1 의 문제와 V2 의 해결
 *  ----------------------------------------------------------------------
 *  - V1 에서는 InterviewService 한 개의 @Transactional 안에서 STT → AI 호출 → DB 저장이
 *    모두 일어났다. AI 호출(수 초) 동안 DB 커넥션이 점유되어 동시 사용자가 늘면 즉시 풀 고갈.
 *  - 같은 빈 내부에서 메서드를 분리해 @Transactional 을 나눠 봤지만, Spring AOP 프록시는
 *    같은 빈 내부 호출에서는 적용되지 않아 트랜잭션이 끊기지 않았다.
 *  - 해결: 별도 빈인 Facade 를 만들어 외부에서 InterviewService 의 짧은 트랜잭션 메서드들을
 *    순서대로 호출. AI 호출 구간은 Facade 위에 놓아 트랜잭션 밖에서 실행되도록 한다.
 *
 *  V3 에서 추가된 것
 *  ----------------------------------------------------------------------
 *  - Mono.zip 으로 독립 AI 호출 병렬화 (STT 정제 + 감정 분석 / 제목 + 다음 오프닝)
 *  - doOnSuccess + 별도 subscribe 로 RAG 적재 / 키워드 저장 등을 메인 응답 체인에서 분리
 *
 * ===========================================================================
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class InterviewFacade {

    private final InterviewService interviewService;
    private final AiUtils aiUtils;
    private final RedisService redisService;
    private final DomainFindUtils domainFindUtils;
    private final ObjectMapper objectMapper;
    private final SimpMessagingTemplate messagingTemplate;
    private final InterviewQuestionRepository interviewQuestionRepository;

    /*
     * ---------------------------------------------------------------
     *  V1 초기 구현 — 동기 처리 버전 (참고용)
     * ---------------------------------------------------------------
     *  WebSocket 으로 들어온 전체 음성을 한 번에 AI 서버에 보내는 V1 방식.
     *  3단계가 모두 트랜잭션으로 분리되어 있어 V1 의 "AI 호출 동안 커넥션 점유" 문제는
     *  해결되었지만, 발화 시간이 길어질수록 응답 시간이 선형으로 증가하는 한계는 남아 있었다.
     *  → V2 청크 STT + V3 reactive 체인으로 대체됨. 비교용으로 남겨둠.
     * ---------------------------------------------------------------
     */
    public FollowUpQuestionDTO processOneQnA(/* AudioMessageRequest request */) {
        // (생략) 트랜잭션 분리 흐름:
        //   1. interviewService.lockQuestionAndSaveAudio(...)   @Transactional — 짧게 끝남
        //   2. aiUtils.callFastApiForNextQuestion(...)          트랜잭션 X — 커넥션 미점유
        //   3. interviewService.saveAiResultAndUnlock(...)      @Transactional — 짧게 끝남
        return null;
    }

    /**
     * 꼬리질문 생성 메인 파이프라인 — V3 reactive 체인.
     * <p>
     * 1. Redis 에서 청크 조립 텍스트와 컨텍스트 DTO 조회
     * 2. Mono.zip 으로 InterviewQuestion / Member 조회 병렬화
     * 3. 종료 조건(첫 사용자 sequence ≥ 6) 만족 시 종료 파이프라인으로 전환
     * 4. AI 서버 호출 후 짧은 트랜잭션으로 결과 저장
     * 5. 메인 응답을 막지 않도록 키워드 추출은 doOnSuccess + 별도 subscribe 로 fire-and-forget
     */
    public Mono<FollowUpQuestionDTO> generateNextQuestionFacade(Integer interviewId,
            Integer questionId, long start, String email) {

        log.info("Facade 레이어 도착");

        // 1. Redis 키 prefix
        String redisKey = "interviewId:" + interviewId + "questionId:" + questionId;

        AiPipelineReadyDto aiPipelineReadyDto = parseDtoFromJson(
                redisService.getValues(redisKey + "aiPipelineReadyDto"));

        String savedPath = redisService.getValues(redisKey + "savedPath");
        String answerText = redisService.getOrderedChunks(redisKey + "chunks");

        log.info("AI 파이프라인 호출 시작");

        // 2. 두 도메인 객체 조회를 병렬로 — 독립적이므로 순차 실행할 이유가 없음
        return Mono.zip(
                        Mono.fromCallable(() -> domainFindUtils.getInterviewQuestionById(questionId))
                                .subscribeOn(Schedulers.boundedElastic()),
                        Mono.fromCallable(() -> domainFindUtils.getMemberByEmail(email))
                                .subscribeOn(Schedulers.boundedElastic())
                )
                .flatMap(tuple -> {
                    InterviewQuestion interviewQuestion = tuple.getT1();
                    Member member = tuple.getT2();

                    // 3. 첫 사용자 + sequence ≥ 6 → 자동 종료 분기
                    if (!member.getIsFirstFinished() && interviewQuestion.getSequence() + 1 >= 6) {
                        log.info("sequence 기준치 도달. 인터뷰 종료 파이프라인으로 전환");
                        return exitInterviewFacade(email, InterviewExitRequest.builder()
                                .interviewId(interviewId)
                                .questionId(questionId)
                                .build())
                                .then(Mono.fromRunnable(() ->
                                        messagingTemplate.convertAndSend(
                                                "/topic/interview/tmp/" + interviewId,
                                                FollowUpQuestionDTO.builder()
                                                        .interviewId(interviewId)
                                                        .questionId(questionId)
                                                        .isExit(true)
                                                        .build()
                                        )));
                    }

                    // 4. AI 파이프라인 호출 → 5. 결과 저장
                    return Mono.fromCallable(() -> Files.readAllBytes(Paths.get(savedPath)))
                            .subscribeOn(Schedulers.boundedElastic())
                            .flatMap(audioBytes ->
                                    aiUtils.callFastApiForNextQuestionV2(audioBytes,
                                            interviewId, questionId,
                                            aiPipelineReadyDto.getSequence() + 1,
                                            aiPipelineReadyDto.getNickname(),
                                            aiPipelineReadyDto.getPreviousQuestion(),
                                            aiPipelineReadyDto.getIsFirstFinished(),
                                            answerText,
                                            aiPipelineReadyDto.getVoice()))
                            .flatMap(aiPipelineResponse -> Mono.fromCallable(() -> {
                                Integer nextQuestionId = interviewService.saveQuestionAndAnswer(
                                        interviewId, questionId,
                                        aiPipelineResponse.getQuestionText(),
                                        savedPath, answerText,
                                        aiPipelineResponse.getInterviewStateDb().getFusedScores());
                                String savedAnswerText = interviewService.getSavedAnswerText(questionId);

                                // 다음 턴을 위한 컨텍스트 DTO 를 Redis 에 캐싱
                                String dto = objectMapper.writeValueAsString(
                                        AiPipelineReadyDto.builder()
                                                .previousQuestion(aiPipelineResponse.getQuestionText())
                                                .isFirstFinished(aiPipelineReadyDto.getIsFirstFinished())
                                                .nickname(aiPipelineReadyDto.getNickname())
                                                .sequence(aiPipelineReadyDto.getSequence() + 1)
                                                .voice(aiPipelineReadyDto.getVoice())
                                                .build());
                                String newRedisKey = "interviewId:" + interviewId + "questionId:" + nextQuestionId;
                                redisService.setValues(newRedisKey + "aiPipelineReadyDto", dto,
                                        Duration.ofMinutes(5));
                                interviewService.cacheInterviewVoice(interviewId, aiPipelineReadyDto.getVoice());

                                // TTL 락 해제 — exitV2(HTTP) 와의 race 보호 종료
                                redisService.deleteValues("interviewId:" + interviewId + "lock");
                                log.info("요청 및 반환 종료. 걸린 시간: {}",
                                        System.currentTimeMillis() - start);

                                return FollowUpQuestionDTO.builder()
                                        .interviewId(interviewId)
                                        .questionId(nextQuestionId)
                                        .sttResult(savedAnswerText)
                                        .followUpQuestion(aiPipelineResponse.getQuestionText())
                                        .ttsAudioBase64(aiPipelineResponse.getTtsAudioBase64())
                                        .build();
                            }).subscribeOn(Schedulers.boundedElastic()))

                            /*
                             * 키워드 추출 — 사용자 응답 체인과 분리.
                             * 메인 체인의 결과는 이미 사용자에게 돌아가야 하므로, 여기서 .subscribe() 를
                             * 따로 호출해 fire-and-forget 으로 처리한다. 실패해도 인터뷰 응답에는 영향 없음.
                             */
                            .doOnSuccess(followUpQuestionDTO ->
                                    aiUtils.callFastApiKeyword(KeywordRequest.builder()
                                                    .answer(answerText)
                                                    .question(interviewQuestion.getQuestionText())
                                                    .build())
                                            .flatMap(keywordResponse ->
                                                    Mono.fromRunnable(() ->
                                                                    interviewService.saveKeyword(
                                                                            interviewQuestion,
                                                                            keywordResponse.getKeyword()))
                                                            .subscribeOn(Schedulers.boundedElastic()))
                                            .subscribe());
                });
    }

    /**
     * 인터뷰 종료 파이프라인.
     * <p>
     * 핵심 — 두 군데에서 Mono.zip 으로 독립 AI 호출을 병렬화:
     *   ① STT 정제 + 감정 분석    (사용자 마지막 답변 처리)
     *   ② 제목 생성 + 다음 오프닝 (대화 전체 컨텍스트 처리)
     * <p>
     * 그리고 RAG 적재는 메인 체인 종료 후 doOnSuccess + 별도 subscribe 로 분리.
     * RAG 가 느려도 사용자 화면 전환을 막지 않는다.
     */
    public Mono<Void> exitInterviewFacade(String email, InterviewExitRequest request) {

        long start = System.currentTimeMillis();

        String redisKey = "interviewId:" + request.getInterviewId()
                + "questionId:" + request.getQuestionId();
        String savedPath = redisService.getValues(redisKey + "savedPath");
        String answerText = redisService.getOrderedChunks(redisKey + "chunks");

        // 람다 캡처 — flatMap 체인을 가로질러 값 공유
        AtomicReference<InterviewQuestion> questionRef = new AtomicReference<>();
        AtomicReference<AiTitleResponse> aiTitleResponseRef = new AtomicReference<>();

        // 1. 디스크에서 음성 원본 로드
        return Mono.fromCallable(() -> Files.readAllBytes(Paths.get(savedPath)))
                .subscribeOn(Schedulers.boundedElastic())

                // 2. 마지막 답변 DB 저장
                .flatMap(bytes -> Mono.fromCallable(() -> {
                            InterviewQuestion question = interviewService.saveAnswer(
                                    request.getQuestionId(), answerText, savedPath);
                            questionRef.set(question);
                            return bytes;
                        }).subscribeOn(Schedulers.boundedElastic())
                )

                // 3. [병렬] STT 정제 + 감정 분석 — 둘 다 마지막 답변에 대한 독립 호출
                .flatMap(bytes -> Mono.zip(
                        aiUtils.lastAnswerStt(answerText),
                        aiUtils.lastAnswerEmotion(answerText, bytes)
                ))

                // 4. 감정/정제 텍스트 저장 + 전체 대화 컨텍스트 조립
                .flatMap(tuple -> Mono.fromCallable(() -> {
                            interviewService.saveEmotionTts(tuple.getT1(), tuple.getT2(), request);
                            return interviewService.findDomainMakeQnACouple(email, request.getInterviewId());
                        }).subscribeOn(Schedulers.boundedElastic())
                )

                // 5. [병렬] 다음 오프닝 + 제목 — 같은 컨텍스트로 동시에 호출
                .flatMap(answerTts -> {
                    if (answerTts.getQnACouple() == null || answerTts.getQnACouple().isEmpty()) {
                        answerTts.protectEmptyString();
                    }

                    Mono<AiTitleResponse> aiTitleResponseMono = aiUtils.callFastApiForInterviewTitleV2(
                            AiTitleRequest.builder()
                                    .interviewContext(answerTts.getQnACouple())
                                    .build());

                    Mono<AiNextOpeningResponse> aiNextOpeningResponseMono = aiUtils.callFastApiForNextOpeningV2(
                            AiNextOpeningRequest.builder()
                                    .interviewContext(answerTts.getQnACouple())
                                    .nickname(answerTts.getMember().getNickname())
                                    .build());

                    return Mono.zip(aiNextOpeningResponseMono, aiTitleResponseMono)
                            // 6. AI 결과 저장 + 사용자에게 SAVE_COMPLETE 푸시
                            .flatMap(aiResponse -> Mono.fromRunnable(() -> {
                                        AiNextOpeningResponse openingQuestionResponse = aiResponse.getT1();
                                        AiTitleResponse titleResponse = aiResponse.getT2();

                                        // RAG 메타데이터(제목)를 turns 에 주입
                                        for (Turn turn : answerTts.getTurns()) {
                                            turn.setMetadata(titleResponse.getTitle());
                                        }
                                        aiTitleResponseRef.set(titleResponse);

                                        interviewService.saveExitAiResponse(
                                                request.getInterviewId(),
                                                titleResponse.getTitle(),
                                                openingQuestionResponse.getDbQuestion());
                                        redisService.deleteValues(
                                                "interviewId:" + request.getInterviewId() + "isSaving");

                                        // 사용자에게 즉시 종료 신호 — 이 시점에 화면이 결과 페이지로 전환됨
                                        messagingTemplate.convertAndSend(
                                                "/topic/interview/tmp/" + request.getInterviewId(),
                                                (Object) Map.of("type", "SAVE_COMPLETE"));
                                    }).subscribeOn(Schedulers.boundedElastic())
                            )
                            .retry(3) // 네트워크 일시 장애 대비

                            /*
                             * RAG 적재 + 키워드 저장 — 사용자 응답과 무관하므로 메인 체인 외부로 분리.
                             * 별도 subscribe 로 fire-and-forget. 실패해도 인터뷰 종료 신호는 이미 나간 뒤.
                             */
                            .doOnSuccess(ignored -> Mono.zip(
                                            aiUtils.callFastApiForRagUpsert(UpsertRequest.builder()
                                                            .userId(String.valueOf(answerTts.getMember().getId()))
                                                            .interviewId(String.valueOf(request.getInterviewId()))
                                                            .turns(answerTts.getTurns())
                                                            .metadata(Map.of("title",
                                                                    aiTitleResponseRef.get().getTitle()))
                                                            .build())
                                                    .doOnError(e -> log.error("RAG upsert 실패: {}", e.getMessage())),
                                            aiUtils.callFastApiKeyword(KeywordRequest.builder()
                                                    .question(questionRef.get().getQuestionText())
                                                    .answer(answerText)
                                                    .build())
                                    ).doOnSuccess(t -> interviewService.saveKeyword(questionRef.get(),
                                            t.getT2().getKeyword()))
                                    .subscribeOn(Schedulers.boundedElastic())
                                    .subscribe(
                                            null,
                                            error -> log.error("키워드 저장 파이프라인 실패: {}", error.getMessage())
                                    ));
                })
                .doFinally(signal -> log.info("[EXIT_TIMING] exitInterviewFacade 종료 (signal={}) — interviewId: {}, 총 소요시간: {}ms",
                        signal, request.getInterviewId(), System.currentTimeMillis() - start))
                .then();
    }

    /**
     * 사용자가 단 한 번도 답변하지 않고 종료한 경우의 라이트 버전.
     * 이 경우에도 제목/다음 오프닝 두 호출은 독립적이므로 Mono.zip 으로 병렬화.
     */
    public void exitInterviewNoAudioFacade(/* TotalChunkRequest request, String email, String redisKey */) {
        // (생략) Mono.zip(callFastApiForNextOpeningV2, callFastApiForInterviewTitleV2)
        //       .flatMap(tuple -> { saveExitAiResponse + redis 락 해제 + SAVE_COMPLETE 푸시 })
        //       .subscribe(null, error -> log.error(...));
    }

    /**
     * 비정상 종료된 인터뷰의 강제 완료 처리.
     * 위 exitInterviewFacade 와 동일한 Mono.zip(제목 + 오프닝) 병렬 패턴을 사용.
     */
    public Mono<Void> forceFinishInterviewFacade(/* String email, Integer interviewId */) {
        // (생략) Mono.zip(aiNextOpeningResponseMono, aiTitleResponseMono)
        //       .flatMap(... saveExitAiResponse ...)
        //       .retry(2)
        //       .doOnSuccess(ignored -> RAG upsert subscribe);
        return Mono.empty();
    }

    /**
     * 인터뷰 첫 질문 TTS 변환 — WebSocket 연결 직전 호출됨.
     * AI 호출(TTS) → DB 저장의 짧은 트랜잭션 메서드 호출의 흐름을 reactive 체인으로 묶음.
     */
    public Mono /* <WebSocketConnectResponse> */ issueWebSocketId(/* ... */) {
        // (생략) aiUtils.callFastApiForStartTts(...).flatMap(resp ->
        //           Mono.fromCallable(() -> interviewService.saveInterviewAndQuestion(...))
        //               .subscribeOn(Schedulers.boundedElastic()));
        return Mono.empty();
    }

    // ----- 헬퍼 -----

    private AiPipelineReadyDto parseDtoFromJson(String json) {
        try {
            return objectMapper.readValue(json, AiPipelineReadyDto.class);
        } catch (JsonProcessingException e) {
            log.info("역직렬화 중 오류 발생");
            throw new GeneralException(ErrorStatus.INTERNAL_SERVER_ERROR);
        }
    }

    // ===========================================================================
    //  생략된 메서드: deleteInterviewFacade — RAG 측 데이터 정리 호출만 수행하는 단순 메서드
    // ===========================================================================
}
