package com.theinterview.global.security.config.websocket;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketTransportRegistration;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

/*
 * ===========================================================================
 *  Tomcat WebSocket Status 1009 버그 대응
 * ===========================================================================
 *  현상
 *    - React + @stomp/stompjs 환경에서만 WebSocket 연결이 1009로 끊기고 세션이 중복 생성
 *    - 동일 시나리오를 바닐라 JS + sockjs-client 로 돌리면 정상
 *
 *  원인
 *    - sockjs-client : 16KB 단위로 분할 전송 → Spring 이 메모리에서 조립 후 컨트롤러로 전달
 *    - @stomp/stompjs: 수백 KB 를 단일 프레임으로 전송 → Tomcat 컨테이너 기본 버퍼(8KB) 초과 시
 *                      STOMP 핸들러에 도달하기도 전에 Tomcat 레벨에서 즉시 1009 반환
 *
 *  Spring WebSocket 의 버퍼 설정 레이어는 두 단계로 분리되어 있다.
 *      클라이언트
 *          ↓
 *      Tomcat Container 레벨   ← 여기가 진짜 원인 (기본 8KB)
 *          ↓
 *      Spring Transport 레벨   ← 흔히 보이는 setMessageSizeLimit 가 여기. 이것만 늘려도 안 풀림
 *          ↓
 *      @MessageMapping 컨트롤러
 *
 *  해결
 *    - Spring transport 레벨 (configureWebSocketTransport) 과
 *      Tomcat container 레벨 (ServletServerContainerFactoryBean) 둘 다 함께 늘려준다.
 * ===========================================================================
 */
@Configuration
@EnableWebSocketMessageBroker
@RequiredArgsConstructor
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    private final StompHandler stompHandler;

    // 클라이언트가 연결을 시도할 최초의 엔드포인트
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws-connect")
                .setAllowedOriginPatterns("*")
                .withSockJS();
    }

    // 메시지 브로커 라우팅 규칙
    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        // 수신: 프론트엔드가 데이터를 받을(구독) 경로의 접두사
        registry.enableSimpleBroker("/topic");
        // 송신: 프론트엔드가 서버로 데이터를 보낼(발행) 경로의 접두사
        registry.setApplicationDestinationPrefixes("/app");
    }

    // 인바운드 채널 — JWT 검증 등 모든 STOMP 메시지를 stompHandler 가 가로챔
    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(stompHandler);
    }

    // [Spring transport 레벨] WebSocket 전송 자체의 메시지/버퍼/타임아웃 설정
    @Override
    public void configureWebSocketTransport(WebSocketTransportRegistration registry) {
        registry
                .setMessageSizeLimit(50 * 1024 * 1024)
                .setSendBufferSizeLimit(50 * 1024 * 1024)
                .setSendTimeLimit(10 * 1000);
    }

    /*
     * [Tomcat container 레벨] 1009 버그의 진짜 해결 지점.
     *  - ServletServerContainerFactoryBean 으로 Tomcat 이 단일 프레임에서 받아들일 수 있는
     *    바이트 한도를 명시적으로 키운다.
     *  - 이 빈 없이 transport 레벨만 늘리면 메시지가 Spring 까지 도달하지 못하고 끊긴다.
     */
    @Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        // 텍스트(Base64 인코딩된 음성) 메시지 버퍼 50MB
        container.setMaxTextMessageBufferSize(50 * 1024 * 1024);
        // 혹시 모를 바이너리 데이터 버퍼 50MB
        container.setMaxBinaryMessageBufferSize(50 * 1024 * 1024);
        // 세션 타임아웃 5분 — 사용자의 발화 시간 확보
        container.setMaxSessionIdleTimeout(300000L);
        return container;
    }
}
