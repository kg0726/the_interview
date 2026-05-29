package com.theinterview.global.config.redis;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisService {

    // RedisConfig를 통해 스프링 빈에 등록해둔 템플릿
    private final RedisTemplate<String, String> redisTemplate;

    /**
     * 데이터를 Redis에 저장함(TTL 설정 포함)
     *
     * @param key      저장할 키
     * @param data     저장할 값
     * @param duration 만료 시간(이 시간이 지나면 Redis에서 자동 삭제)
     */
    public void setValues(String key, String data, Duration duration) {
        ValueOperations<String, String> values = redisTemplate.opsForValue();
        values.set(key, data, duration);
        log.info("Redis에 데이터 저장 완료 Key: {}, TTL: {}초", key, duration.getSeconds());
    }

    /**
     * 키를 기반으로 Redis에서 데이터를 조회함
     */
    public String getValues(String key) {
        ValueOperations<String, String> values = redisTemplate.opsForValue();
        return values.get(key);
    }

    public void deleteValues(String key) {
        redisTemplate.delete(key);
        log.info("Redis에서 데이터 삭제 완료 - Key: {}", key);
    }

    public boolean hasKey(String key) {
        return redisTemplate.hasKey(key);
    }

    // 왜 Sorted Set을 선택하였는가?
    // WebClient의 비동기 통신 특성 상 3번 청크가 2번 청크보다 먼저 addChunkToSortedSet을 호출하더라도
    // Redis 내부에서 알아서 index(score)를 기준으로 줄을 세워 줌 — 도착 순서에 무관하게 발화 순서가 보장됨

    /**
     * STT 청크 데이터를 순서(Index)를 보장하여 Sorted Set에 저장함
     *
     * @param key        Redis Key (예: interview:1:question:5:chunks)
     * @param chunkText  STT로 변환된 텍스트
     * @param chunkIndex 프론트엔드에서 보낸 청크의 순서 (Score로 사용됨)
     */
    public void addChunkToSortedSet(String key, String chunkText, double chunkIndex) {

        // ZSet(Sorted Set) 오퍼레이션 호출
        ZSetOperations<String, String> zSetOps = redisTemplate.opsForZSet();

        // 인덱스 번호까지 같이 저장(value 겹침 방지 — 동일 텍스트 청크가 두 번 나오면 ZSet member가 충돌함)
        String valueWithIndex = (int) chunkIndex + ":" + chunkText;
        // add(key, value, score) - score 기준으로 자동 오름차순 정렬
        zSetOps.add(key, valueWithIndex, chunkIndex);
        redisTemplate.expire(key, Duration.ofSeconds(30)); // 매 청크마다 TTL 갱신
        log.info("Redis Sorted Set에 청크 저장 완료 - Key: {}, Index: {}", key, chunkIndex);
    }

    /**
     * Sorted Set에 저장된 모든 청크 텍스트를 순서대로 조합하여 반환함
     */
    public String getOrderedChunks(String key) {
        ZSetOperations<String, String> zSetOps = redisTemplate.opsForZSet();

        // range(key, 0, -1)은 마지막까지 Score 기준 오름차순 정렬
        Set<String> orderedChunks = zSetOps.range(key, 0, -1);

        if (orderedChunks == null || orderedChunks.isEmpty()) {
            return "";
        }

        // 문자열을 하나로 이어붙여 반환함(저장할 때 붙였던 인덱스 번호를 제외함)
        return orderedChunks.stream()
                .map(chunk -> chunk.substring(chunk.indexOf(":") + 1))
                .collect(Collectors.joining(" "));
    }

    public Long savedChunkCount(String key) {
        return redisTemplate.opsForZSet().size(key);
    }

    public void saveAllChuckCount(String key, int totalChunks) {
        ValueOperations<String, String> values = redisTemplate.opsForValue();
        values.set(key, String.valueOf(totalChunks));
    }

    /**
     * 키가 없을 때만 저장 (Redis SETNX) — 파이프라인 중복 트리거 방지용.
     * <p>
     * 사용 시나리오: 마지막 청크 STT 완료(WebSocket 경로) 와 /next totalChunks 수신(WebSocket 경로) 또는
     * /exitV2 수신(HTTP 경로)이 거의 동시에 도착할 때, 두 경로 모두 완료 조건을 검사하지만
     * 파이프라인은 정확히 한 번만 실행되어야 한다. setIfAbsent 가 true 를 반환한 쪽만 트리거를 점유한다.
     */
    public boolean setIfAbsent(String key, String value, Duration duration) {
        ValueOperations<String, String> values = redisTemplate.opsForValue();
        Boolean result = values.setIfAbsent(key, value, duration);
        return Boolean.TRUE.equals(result);
    }
}
