package com.video.stream.config;

import com.video.stream.service.IRtspStreamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;

@Component
public class HlsAccessInterceptor implements HandlerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(HlsAccessInterceptor.class);
    private static final int M3U8_WAIT_RETRY_COUNT = 50;
    private static final long M3U8_WAIT_SLEEP_MS = 100L;

    @Value("${rtsp.hls.output-path:D:/video/hls}")
    private String hlsOutputPath;

    @Value("${rtsp.hls.access-path:/rtspStream/hls}")
    private String hlsAccessPath;

    @Autowired(required = false)
    private IRtspStreamService streamService;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (streamService == null) {
            return true;
        }

        String uri = request.getRequestURI();
        String prefix = hlsAccessPath.endsWith("/") ? hlsAccessPath : hlsAccessPath + "/";
        if (uri == null || !uri.startsWith(prefix)) {
            return true;
        }

        String pathAfterPrefix = uri.substring(prefix.length());
        int slashIndex = pathAfterPrefix.indexOf('/');
        if (slashIndex <= 0) {
            return true;
        }

        String streamId = pathAfterPrefix.substring(0, slashIndex);
        if (streamId.isEmpty() || streamId.contains("..")) {
            return true;
        }

        if (!streamService.isStreamManaged(streamId)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return false;
        }

        streamService.recordStreamAccess(streamId);

        if (!pathAfterPrefix.endsWith("/index.m3u8")) {
            return true;
        }

        streamService.tryRecoverStream(streamId);

        File m3u8File = new File(hlsOutputPath, streamId + "/index.m3u8");
        for (int i = 0; i < M3U8_WAIT_RETRY_COUNT; i++) {
            if (!streamService.isStreamManaged(streamId)) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return false;
            }
            if (m3u8File.exists() && m3u8File.length() > 0) {
                return true;
            }
            try {
                Thread.sleep(M3U8_WAIT_SLEEP_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        log.warn("[HLS-{}] m3u8 file was not created in time after recovery", streamId);
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        return false;
    }
}
