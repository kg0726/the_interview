package com.theinterview.domain.interview.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.theinterview.domain.interview.Repository.InterviewAnswerRepository;
import com.theinterview.domain.interview.Repository.InterviewQuestionRepository;
import com.theinterview.domain.interview.Repository.InterviewRepository;
import com.theinterview.domain.interview.dto.AiRagDto.Turn;
import com.theinterview.domain.interview.dto.FacadeInnerDto.AnswerTts;
import com.theinterview.domain.interview.dto.FastApiRequest.AiPipelineReadyDto;
import com.theinterview.domain.interview.dto.FastApiResponse.AiPipelineResponse.InterviewStateDb.FusedScores;
import com.theinterview.domain.interview.dto.FastApiResponse.AiStartTtsResponse;
import com.theinterview.domain.interview.dto.FastApiResponse.LastAnswerEmotionResponse;
import com.theinterview.domain.interview.dto.FastApiResponse.LastAnswerTextResponse;
import com.theinterview.domain.interview.dto.InterviewRequest.AudioChunkRequest;
import com.theinterview.domain.interview.dto.InterviewRequest.AudioMessageRequest;
import com.theinterview.domain.interview.dto.InterviewRequest.InterviewExitRequest;
import com.theinterview.domain.interview.dto.InterviewResponse.FollowUpQuestionDTO;
import com.theinterview.domain.interview.dto.InterviewResponse.WebSocketConnectResponse;
import com.theinterview.domain.interview.entity.Interview;
import com.theinterview.domain.interview.entity.InterviewAnswer;
import com.theinterview.domain.interview.entity.InterviewQuestion;
import com.theinterview.domain.interview.entity.QuestionCandidate;
import com.theinterview.domain.interview.enums.InterviewStatus;
import com.theinterview.domain.member.Member;
import com.theinterview.global.config.redis.RedisService;
import com.theinterview.global.response.exception.GeneralException;
import com.theinterview.global.response.status.ErrorStatus;
import com.theinterview.global.util.DomainFindUtils;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

/*
 * ===========================================================================
 *  InterviewService — 짧은 트랜잭션 단위 메서드 모음
 * ===========================================================================
 *
 *  설계 원칙
 *  ----------------------------------------------------------------------
 *  - 한 트랜잭션 안에서 절대 외부 네트워크(AI 호출, RAG 호출 등) 를 직접 부르지 않는다.
 *    AI 호출은 항상 Facade 쪽에서, 트랜잭션 밖에서 실행된다.
 *  - 각 @Transactional 메서드는 짧게 끝난다 → DB 커넥션 점유 시간을 최소화.
 *  - 비동기 STT 응답이 도착 순서와 무관하게 합쳐지도록 Redis Sorted Set 을 활용한다.
 *  - "답변"과 "꼬리질문 응답"이 거의 동시에 도착하는 race 상황을
 *    Redis 임시 저장 + DB 저장 패턴으로 해결한다.
 *
 * ===========================================================================
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class InterviewService {

    private static final Duration INTERVIEW_VOICE_TTL = Duration.ofHours(6);

    /*
     * 정제 답변 텍스트는 꼬리질문 응답보다 먼저 도착할 수 있으므로,
     * 실제 InterviewAnswer 행이 만들어지기 전까지는 Redis 에 임시 보관한다.
     * TTL 너무 짧으면 네트워크 지연 시 merge 전에 사라지고, 너무 길면 불필요한 임시 데이터가 남으므로
     * 인터뷰 한 턴 처리 시간을 감안해 10분으로 둔다.
     */
    private static final Duration REFINED_ANSWER_TEMP_TTL = Duration.ofMinutes(10);

    private final InterviewRepository interviewRepository;
    private final InterviewQuestionRepository interviewQuestionRepository;
    private final InterviewAnswerRepository interviewAnswerRepository;

    private final AiUtils aiUtils;
    private final DomainFindUtils domainFindUtils;
    private final RedisService redisService;
    private final ObjectMapper objectMapper;

    @Value("${file.audio.base-path}")
    private String answerAudioPath;

    // ===========================================================================
    //  1. 인터뷰 시작 — 첫 질문 + 인터뷰 객체 생성
    // ===========================================================================

    @Transactional
    public WebSocketConnectResponse saveInterviewAndQuestion(
            Member member, AiStartTtsResponse aiStartTtsResponse, String voice,
            Boolean restartFlag, Integer interviewId
    ) throws JsonProcessingException {
        // (생략) restartFlag 분기 후 interview / question 엔티티 save → 다음 턴용 컨텍스트 DTO 를
        //       Redis 에 직렬화 저장 → WebSocketConnectResponse 반환
        return null;
    }

    // ===========================================================================
    //  2. V1 동기 흐름의 짧은 트랜잭션 3단계
    //     Facade 가 이 3개를 외부에서 순서대로 호출하므로 트랜잭션이 정확히 분리된다
    // ===========================================================================

    /**
     * [트랜잭션] 사용자 답변 음성 파일을 디스크에 저장하고, 해당 질문 행에 IN_PROGRESS 상태 락을 건다.
     * <p>
     * 락 자체는 DB 상태 컬럼이지만, 같은 인터뷰의 다른 질문이 동시에 진입하는 경우만을 막기 위한 용도다.
     * (서로 다른 인터뷰 간 race condition 보호는 별도로 Redis TTL 락이 담당한다.)
     */
    @Transactional
    public AiPipelineReadyDto lockQuestionAndSaveAudio(AudioMessageRequest request, byte[] audioBytes)
            throws IOException {

        InterviewQuestion question = interviewQuestionRepository.findById(request.getQuestionId())
                .orElseThrow(() -> new GeneralException(ErrorStatus.QUESTION_NOT_FOUND));
        Interview interview = question.getInterview();
        Member member = interview.getMember();

        if (question.getStatus() == InterviewStatus.IN_PROGRESS) {
            throw new GeneralException(ErrorStatus.QUESTION_NOT_SAVED);
        }
        question.interviewQuestionInProgress();

        String savedFilePath = saveAnswerFile(audioBytes, interview.getId());
        AiPipelineReadyDto previousReadyDto = getReadyDto(request.getInterviewId(), request.getQuestionId());

        return AiPipelineReadyDto.builder()
                .savedFilePath(savedFilePath)
                .sequence(question.getSequence() + 1)
                .nickname(member.getNickname())
                .previousQuestion(question.getQuestionText())
                .isFirstFinished(member.getIsFirstFinished())
                .voice(previousReadyDto.getVoice())
                .build();
    }

    /**
     * [트랜잭션] 사용자 답변 + 생성된 꼬리질문을 한 번에 저장.
     * V3 reactive 체인의 "결과 저장" 단계에서 호출됨.
     */
    @Transactional
    public Integer saveQuestionAndAnswer(Integer interviewId, Integer questionId,
            String generatedQuestionText, String savedPath, String answerText,
            FusedScores fusedScores) {

        InterviewQuestion question = domainFindUtils.getInterviewQuestionById(questionId);
        Interview interview = domainFindUtils.getInterviewById(interviewId);
        String resolvedAnswerText = resolveAnswerText(interviewId, questionId, answerText);

        interviewAnswerRepository.save(InterviewAnswer.builder()
                .interviewQuestion(question)
                .answerText(resolvedAnswerText)
                .answerAudioUrl(savedPath)
                .calmScore(conversionEmotionScore(fusedScores.getCalm()))
                .engagedScore(conversionEmotionScore(fusedScores.getEngaged()))
                .lowScore(conversionEmotionScore(fusedScores.getLow()))
                .anxiousScore(conversionEmotionScore(fusedScores.getAnxious()))
                .tenseScore(conversionEmotionScore(fusedScores.getTense()))
                .build());

        InterviewQuestion saveQuestion = interviewQuestionRepository.save(
                InterviewQuestion.builder()
                        .interview(interview)
                        .sequence(question.getSequence() + 1)
                        .questionText(generatedQuestionText)
                        .build());

        return saveQuestion.getId();
    }

    /**
     * [트랜잭션] V1 의 "AI 결과 저장 + DB 락 해제" — V2/V3 로 이행하면서 saveQuestionAndAnswer 가
     * 사실상 같은 역할을 하지만, 첫 사용자 자동 종료 분기(sequence ≥ 6) 처리 차이 때문에 남아 있음.
     */
    @Transactional
    public FollowUpQuestionDTO saveAiResultAndUnlock(/* AudioMessageRequest, AiPipelineResponse, savedFilePath */) {
        // (생략) InterviewAnswer save → 첫 사용자 + sequence ≥ 6 이면 종료, 아니면 다음 질문 save
        //       → question.interviewQuestionFinished() (DB 락 해제) → DTO 반환
        return null;
    }

    // ===========================================================================
    //  3. 청크 STT — Redis Sorted Set 으로 순서 보장
    // ===========================================================================

    /**
     * 청크된 음성 조각을 FastAPI STT 로 비동기 호출하고, 결과를 Redis Sorted Set 에 적재한다.
     * <p>
     * Sorted Set 의 score 로 청크 인덱스를 사용하기 때문에, WebClient 의 응답이 도착 순서와 무관하게
     * Redis 내부에서 자동 정렬된다. → 발화 시간이 길어져도 응답 시간이 선형으로 늘어나는 V1 의 문제 해결.
     * <p>
     * 반환값 true → 모든 청크가 도착했고(완료 조건 만족), 호출자에게 파이프라인 트리거 시점임을 알림.
     * 단, 진짜 트리거는 컨트롤러에서 SETNX 로 한 번 더 점유 검사를 거친 뒤에 일어난다.
     */
    public Mono<Boolean> chunkAudioSTT(AudioChunkRequest request) {

        return aiUtils.callFastApiForSttChunk(request).map(sttChunkResponse -> {
                    String redisKey = "interviewId:" + request.getInterviewId()
                            + "questionId:" + request.getQuestionId();

                    // ZSet 적재 — score = chunkIndex 이므로 순서가 자동으로 보장됨
                    redisService.addChunkToSortedSet(
                            redisKey + "chunks",
                            sttChunkResponse.getChunkText(),
                            request.getChunkIndex()
                    );
                    log.info("청킹된 오디오 조각 STT 변환 성공 및 redis 저장: {}", sttChunkResponse.getChunkText());

                    // /next 이벤트로 totalChunks 가 이미 수신된 상태라면 완료 조건 검사
                    if (redisService.hasKey(redisKey + "totalChunks")) {
                        int totalChunks = Integer.parseInt(redisService.getValues(redisKey + "totalChunks"));
                        if (totalChunks == redisService.savedChunkCount(redisKey + "chunks")) {
                            return true;
                        }
                    }
                    return false;
                }).doOnError(e -> log.error("청크 변환 실패: {}", e.getMessage()))
                .onErrorReturn(false);
    }

    public void saveRequiredFlagInRedis(String savedPath, String redisKey, String totalChunk) {
        redisService.setValues(redisKey + "totalChunks", totalChunk, Duration.ofMinutes(3));
        redisService.setValues(redisKey + "savedPath", savedPath, Duration.ofMinutes(3));
    }

    // ===========================================================================
    //  4. refined-answer race 처리 — Redis 임시 저장 → DB merge
    // ===========================================================================

    /*
     * AI 서버는 STT 정제 텍스트를 별도 콜백으로 다시 보내준다. 그런데 이 콜백이
     * 메인 꼬리질문 응답보다 먼저 도착할 수도, 나중에 도착할 수도 있다.
     *
     *   case 1) 콜백 먼저 → InterviewAnswer 행이 아직 없음 → Redis 에 임시 저장
     *   case 2) 응답 먼저 → InterviewAnswer 행 존재 → 곧장 DB update
     *
     * resolveAnswerText 가 두 케이스를 모두 흡수한다.
     */

    private String buildRefinedAnswerRedisKey(Integer interviewId, Integer questionId) {
        // 동일 사용자가 여러 인터뷰 동시 진행, 또는 questionId 가 우연히 겹치는 케이스에서도
        // 임시 텍스트가 충돌하지 않도록 두 ID 를 모두 키에 포함
        return "interviewId:" + interviewId + "questionId:" + questionId + "refinedAnswer";
    }

    @Transactional
    public void saveRefinedAnswer(Integer interviewId, Integer questionId, String refinedAnswer) {
        // 서버 간 콜백 요청이지만, 잘못된 매핑으로 다른 인터뷰 답변을 덮어쓰지 않도록 questionId 의 소속을 검증
        InterviewQuestion question = domainFindUtils.getInterviewQuestionById(questionId);
        if (!question.getInterview().getId().equals(interviewId)) {
            throw new GeneralException(ErrorStatus.FORBIDDEN);
        }

        String normalizedRefinedAnswer = refinedAnswer == null ? "" : refinedAnswer.trim();
        if (normalizedRefinedAnswer.isBlank()) {
            throw new GeneralException(ErrorStatus.BAD_REQUEST);
        }

        InterviewAnswer existingAnswer = interviewAnswerRepository.findByInterviewQuestionId(questionId)
                .orElse(null);

        if (existingAnswer != null) {
            // case 2 — 행이 있으니 DB 만 갱신하고 Redis 임시값은 정리
            existingAnswer.updateAnswerText(normalizedRefinedAnswer);
            redisService.deleteValues(buildRefinedAnswerRedisKey(interviewId, questionId));
            return;
        }

        // case 1 — 행이 없으니 Redis 에 임시 보관, 추후 saveQuestionAndAnswer 에서 merge
        redisService.setValues(
                buildRefinedAnswerRedisKey(interviewId, questionId),
                normalizedRefinedAnswer,
                REFINED_ANSWER_TEMP_TTL
        );
    }

    /**
     * 답변 저장 시 공통으로 호출하는 텍스트 결정 함수.
     * 우선순위: Redis 정제 텍스트 > 호출자가 들고 있는 fallback 원문.
     * Redis 값을 사용한 경우 즉시 삭제해 다음 질문에 재사용되지 않게 한다.
     */
    private String resolveAnswerText(Integer interviewId, Integer questionId, String fallbackAnswerText) {
        String redisKey = buildRefinedAnswerRedisKey(interviewId, questionId);
        String refinedAnswer = redisService.getValues(redisKey);
        if (refinedAnswer != null && !refinedAnswer.isBlank()) {
            redisService.deleteValues(redisKey);
            return refinedAnswer;
        }

        /*
         * 기본 pipeline 경로처럼 AI 서버가 STT 부터 모두 처리하는 흐름에서는 백엔드가 fallback 원문을
         * 갖고 있지 않을 수 있다. 콜백이 거의 동시에 도착했지만 아직 Redis 반영 전일 가능성이 있으므로,
         * 짧게 폴링한 뒤 그래도 없으면 fallback 으로 저장한다.
         */
        if (fallbackAnswerText == null || fallbackAnswerText.isBlank()) {
            for (int retry = 0; retry < 5; retry++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                refinedAnswer = redisService.getValues(redisKey);
                if (refinedAnswer != null && !refinedAnswer.isBlank()) {
                    redisService.deleteValues(redisKey);
                    return refinedAnswer;
                }
            }
        }
        return fallbackAnswerText;
    }

    public String getSavedAnswerText(Integer questionId) {
        // 응답 직전 — DB 가 가장 최종 상태이므로 다시 읽어서 사용자에게 노출
        return interviewAnswerRepository.findByInterviewQuestionId(questionId)
                .map(InterviewAnswer::getAnswerText)
                .orElse("");
    }

    // ===========================================================================
    //  5. 종료 흐름 보조 — 마지막 답변 저장 / 컨텍스트 조립 / 종료 시 AI 결과 저장
    // ===========================================================================

    @Transactional
    public InterviewQuestion saveAnswer(Integer questionId, String answerText, String savedPath) {
        InterviewQuestion interviewQuestion = domainFindUtils.getInterviewQuestionById(questionId);
        String resolvedAnswerText = resolveAnswerText(
                interviewQuestion.getInterview().getId(), questionId, answerText
        );
        interviewAnswerRepository.save(InterviewAnswer.builder()
                .interviewQuestion(interviewQuestion)
                .answerText(resolvedAnswerText)
                .answerAudioUrl(savedPath)
                .build());
        return interviewQuestion;
    }

    /** 파이프라인 에러 시 저장된 답변 정리 — 재시도 시 중복 저장 방지 */
    @Transactional
    public void deleteAnswerByQuestionId(Integer questionId) {
        interviewAnswerRepository.deleteAllByInterviewQuestionId(questionId);
    }

    /**
     * 종료 시 호출 — 해당 인터뷰의 전체 Q/A 쌍을 LLM 컨텍스트 문자열과 RAG 적재용 turns 로 동시에 빌드.
     * 답변이 없는 질문은 이 시점에 정리한다.
     */
    @Transactional
    public AnswerTts findDomainMakeQnACouple(String email, Integer interviewId) {

        Interview interview = domainFindUtils.getInterviewById(interviewId);
        Member member = interview.getMember();
        if (!member.getEmail().equals(email)) {
            throw new GeneralException(ErrorStatus.FORBIDDEN);
        }

        String aiRequest = "";
        int counting = 1;
        List<Turn> turns = new ArrayList<>();

        List<InterviewQuestion> interviewQuestions = interview.getInterviewQuestions();
        for (InterviewQuestion interviewQuestion : interviewQuestions) {
            InterviewAnswer interviewAnswer = interviewQuestion.getInterviewAnswer();
            // 사용자가 마지막에 답변하지 않고 종료한 경우 해당 질문은 삭제
            if (interviewAnswer == null) {
                interviewQuestionRepository.delete(interviewQuestion);
                continue;
            }
            aiRequest = aiRequest
                    + "Q" + counting + interviewQuestion.getQuestionText() + "\n"
                    + "A" + counting + interviewAnswer.getAnswerText() + "\n\n";

            turns.add(Turn.builder()
                    .questionId(String.valueOf(interviewQuestion.getId()))
                    .answerId(String.valueOf(interviewAnswer.getId()))
                    .question(interviewQuestion.getQuestionText())
                    .answer(interviewAnswer.getAnswerText())
                    .turnIndex(counting)
                    .metadata(Map.of())
                    .build());
            counting += 1;
        }

        return AnswerTts.builder()
                .QnACouple(aiRequest)
                .member(member)
                .turns(turns)
                .build();
    }

    @Transactional
    public void saveExitAiResponse(Integer interviewId, String title, String nextStartQuestion) {
        // (생략) 인터뷰 제목 update + member.completeFirstInterview() + 다음 회차의 첫 질문 후보 저장
        //       + 공유 보드 알림(있는 경우)
    }

    @Transactional
    public void saveEmotionTts(LastAnswerTextResponse lastAnswerTextResponse,
            LastAnswerEmotionResponse lastAnswerEmotionResponse, InterviewExitRequest request) {
        InterviewQuestion question = domainFindUtils.getInterviewQuestionById(request.getQuestionId());
        question.getInterviewAnswer().updateTextEmotion(
                lastAnswerTextResponse.getRefinedAnswer(),
                lastAnswerEmotionResponse.getInterviewStateDb());
    }

    // ===========================================================================
    //  6. 보조 — 음성 voice 캐싱 / DTO 조회 / 키워드 저장 / 파일 저장
    // ===========================================================================

    @Transactional(readOnly = true)
    public AiPipelineReadyDto getReadyDto(Integer interviewId, Integer questionId) {
        String redisKey = "interviewId:" + interviewId + "questionId:" + questionId + "aiPipelineReadyDto";
        String dtoJson = redisService.getValues(redisKey);
        if (dtoJson != null && !dtoJson.isBlank()) {
            try {
                return objectMapper.readValue(dtoJson, AiPipelineReadyDto.class);
            } catch (JsonProcessingException e) {
                log.warn("AiPipelineReadyDto 역직렬화 실패 — DB fallback 사용", e);
            }
        }
        // (생략) DB fallback — interview/question/member 조회 후 DTO 빌드
        return null;
    }

    public void cacheInterviewVoice(Integer interviewId, String voice) {
        if (voice == null || voice.isBlank()) {
            return;
        }
        redisService.setValues("interviewId:" + interviewId + "voice", voice, INTERVIEW_VOICE_TTL);
    }

    @Transactional
    public void saveKeyword(InterviewQuestion question, String keyword) {
        // 영속성 컨텍스트에 다시 올리기 위해 조회
        InterviewQuestion findQuestion = domainFindUtils.getInterviewQuestionById(question.getId());
        findQuestion.getInterviewAnswer().updateKeyword(keyword);
    }

    @Transactional
    public String saveAnswerFile(byte[] audioBytes, Integer interviewId) throws IOException {
        Interview interview = domainFindUtils.getInterviewById(interviewId);
        Member member = interview.getMember();

        String fileName = UUID.randomUUID() + ".webm";
        Path directoryPath = Paths.get(answerAudioPath, String.valueOf(member.getId()),
                String.valueOf(interviewId));

        if (!Files.exists(directoryPath)) {
            Files.createDirectories(directoryPath);
        }
        Path filePath = directoryPath.resolve(fileName);
        Files.write(filePath, audioBytes);

        return filePath.toAbsolutePath().toString();
    }

    private BigDecimal conversionEmotionScore(BigDecimal score) {
        return score == null ? BigDecimal.ZERO : score;
    }

    // ===========================================================================
    //  생략된 메서드들
    //
    //   - exitInterview (V1 동기 종료) — V2/V3 facade.exitInterviewFacade 로 대체됨
    //   - getInterviewHistoryList / getInterviewHistoryDetail / getSharedInterviewDetail
    //     getSharedPostItBoard / getInterviewExplore (인터뷰 조회/탐색 페이지)
    //   - setInterviewShareRange / getInterviewShareLink / setBookmark / ensureBookmarkLayout
    //     (공유, 북마크, 포스트잇 보드)
    //   - getVoiceResource (인터뷰 음성 스트리밍)
    //   - deleteInterview (인터뷰 + 음성 파일 삭제)
    //   - findUnfinishedInterviews (비정상 종료 이어가기 체크)
    //   - makeAnswerQuestionCouple (오디오 없는 종료 분기 보조)
    // ===========================================================================
}
