package com.video.stream.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.websocket.*;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ServerEndpoint("/ws/flv/{streamId}")
@Component
public class FlvWebSocketServer {
    
    private static final Logger log = LoggerFactory.getLogger(FlvWebSocketServer.class);
    
    private static final Map<String, Session> sessionMap = new ConcurrentHashMap<>();
    
    @OnOpen
    public void onOpen(Session session, @PathParam("streamId") String streamId) {
        sessionMap.put(streamId, session);
        log.info("WebSocket 连接打开: streamId={}, sessionId={}", streamId, session.getId());
    }
    
    @OnClose
    public void onClose(Session session, @PathParam("streamId") String streamId) {
        sessionMap.remove(streamId);
        log.info("WebSocket 连接关闭: streamId={}, sessionId={}", streamId, session.getId());
    }
    
    @OnError
    public void onError(Session session, Throwable error, @PathParam("streamId") String streamId) {
        log.error("WebSocket 错误: streamId={}, error={}", streamId, error.getMessage(), error);
        sessionMap.remove(streamId);
        try {
            session.close();
        } catch (IOException e) {
            log.error("关闭 WebSocket 会话失败", e);
        }
    }
    
    @OnMessage
    public void onMessage(String message, @PathParam("streamId") String streamId) {
        log.debug("收到 WebSocket 消息: streamId={}, message={}", streamId, message);
    }
    
    public void sendFlvData(String streamId, byte[] data) {
        Session session = sessionMap.get(streamId);
        if (session != null && session.isOpen()) {
            try {
                session.getBasicRemote().sendBinary(ByteBuffer.wrap(data));
            } catch (IOException e) {
                log.error("发送 FLV 数据失败: streamId={}", streamId, e);
                sessionMap.remove(streamId);
                try {
                    session.close();
                } catch (IOException ex) {
                    log.error("关闭 WebSocket 会话失败", ex);
                }
            }
        }
    }
    
    public boolean isSessionActive(String streamId) {
        Session session = sessionMap.get(streamId);
        return session != null && session.isOpen();
    }
    
    public void closeSession(String streamId) {
        Session session = sessionMap.remove(streamId);
        if (session != null && session.isOpen()) {
            try {
                session.close();
            } catch (IOException e) {
                log.error("关闭 WebSocket 会话失败: streamId={}", streamId, e);
            }
        }
    }
    
    public static Map<String, Session> getSessionMap() {
        return sessionMap;
    }
}