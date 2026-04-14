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

/**
 * 拦截 HLS 资源请求，在返回静态文件前：
 * 1. 尝试恢复已停止的 FFmpeg 进程
 * 2. 等待 FFmpeg 创建好 m3u8 文件
 */
@Component
public class HlsAccessInterceptor implements HandlerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(HlsAccessInterceptor.class);

    @Value("${rtsp.hls.output-path:D:/video/hls}")
    private String hlsOutputPath;

    @Autowired(required = false)
    private IRtspStreamService streamService;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (streamService == null) {
            return true;
        }

        String uri = request.getRequestURI();
        String prefix = "/rtspStream/hls/";
        if (uri == null || !uri.startsWith(prefix)) {
            return true;
        }

        // 从 URI 提取 streamId: /rtspStream/hls/{streamId}/...
        String pathAfterPrefix = uri.substring(prefix.length());
        int slashIndex = pathAfterPrefix.indexOf('/');
        if (slashIndex <= 0) {
            return true;
        }
        String streamId = pathAfterPrefix.substring(0, slashIndex);
        if (streamId.isEmpty() || streamId.contains("..")) {
            return true;
        }

        // 所有 HLS 请求（包括 .ts 分片）都记录访问时间，防止因无访问记录而被自动停止
        streamService.recordStreamAccess(streamId);

        // 仅对 m3u8 请求做进程恢复和等待
        if (pathAfterPrefix.endsWith("/index.m3u8")) {
            // 尝试恢复已停止的流
            streamService.tryRecoverStream(streamId);

            // FFmpeg 恢复后需要时间创建 m3u8 文件，最多等待3秒
            File m3u8File = new File(hlsOutputPath, streamId + "/index.m3u8");
            for (int i = 0; i < 30; i++) {
                if (m3u8File.exists() && m3u8File.length() > 0) {
                    return true;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            log.warn("[HLS-{}] FFmpeg恢复后m3u8文件仍未就绪，返回404", streamId);
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return false;
        }

        return true;
    }
}
