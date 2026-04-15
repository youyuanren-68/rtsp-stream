package com.video.stream.controller;

import com.video.stream.service.IRtspStreamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

@RestController
@RequestMapping("/rtspStream/flv")
public class FlvStreamController {

    private static final Logger log = LoggerFactory.getLogger(FlvStreamController.class);

    @Value("${rtsp.flv.output-path:D:/video/flv}")
    private String flvOutputPath;

    @Autowired
    private IRtspStreamService streamService;

    /**
     * 同步 void 方法 —— 不返回任何 Spring 异步类型，
     * 所有 write 异常在方法内部消化，不传播到 Spring/Tomcat。
     */
    @GetMapping("/{streamId}/live.flv")
    public void streamFlv(@PathVariable String streamId,
                          HttpServletRequest request,
                          HttpServletResponse response) {

        // 恢复已停止的流
        streamService.tryRecoverStream(streamId);

        // FFmpeg 恢复后需要时间创建文件，最多等待3秒
        File flvFile = null;
        for (int i = 0; i < 30; i++) {
            Path flvPath = Paths.get(flvOutputPath, streamId, "live.flv").normalize();
            File f = flvPath.toFile();
            if (f.exists() && f.length() > 0) {
                flvFile = f;
                break;
            }
            try { Thread.sleep(100); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        // 文件未找到：流不活跃且无法恢复，返回404
        if (flvFile == null) {
            if (!streamService.isStreamActive(streamId)) {
                log.warn("[FLV-{}] FLV文件不存在且流不活跃，返回404", streamId);
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return;
            }
            // 流活跃但文件还未创建，再短暂等待
            for (int i = 0; i < 20; i++) {
                Path flvPath = Paths.get(flvOutputPath, streamId, "live.flv").normalize();
                File f = flvPath.toFile();
                if (f.exists() && f.length() > 0) {
                    flvFile = f;
                    break;
                }
                try { Thread.sleep(100); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            if (flvFile == null) {
                log.warn("[FLV-{}] 流活跃但FLV文件创建超时(2秒)，返回404", streamId);
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return;
            }
        }

        // 文件存在但流不活跃：可能是服务器重启后的残留旧文件，不读取
        if (!streamService.isStreamActive(streamId)) {
            log.warn("[FLV-{}] FLV文件存在但流未启动（可能是残留旧文件），返回404", streamId);
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        response.setContentType("video/x-flv");
        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("Pragma", "no-cache");
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Connection", "keep-alive");
        response.setHeader("Transfer-Encoding", "chunked");

        streamService.recordStreamAccess(streamId);

        // 同步流式传输，所有异常本地消化
        RandomAccessFile raf = null;
        OutputStream out = null;
        try {
            out = response.getOutputStream();
            raf = new RandomAccessFile(flvFile, "r");

            byte[] buffer = new byte[8192];
            int noDataCount = 0;
            int maxNoDataCount = 36000;
            long lastAccessUpdate = System.currentTimeMillis();
            long lastFileSize = 0;
            int stableCount = 0;
            // 给足 30 分钟容忍 FFmpeg 重启/写入间隙
            int stableCountLimit = 18000;
            long totalBytesSent = 0;

            // 文件切换/恢复检测：当文件不增长时触发
            int fileSwitchThreshold = 300; // 300 * 20ms = 6秒
            long lastRecoveryCheck = 0;

            log.info("[FLV-{}] 开始流式传输: {}", streamId, flvFile.getAbsolutePath());

            while (noDataCount < maxNoDataCount) {
                long fileLength = raf.length();
                long currentPosition = raf.getFilePointer();

                if (currentPosition >= fileLength) {
                    // 文件无新数据时，每 1 秒检测一次 FFmpeg 进程是否存活
                    if (stableCount % 50 == 0 && !streamService.isStreamActive(streamId)) {
                        log.warn("[FLV-{}] FFmpeg进程已退出但文件未增长，结束传输", streamId);
                        break;
                    }

                    if (fileLength == lastFileSize) {
                        stableCount++;
                        if (stableCount > stableCountLimit) {
                            // FFmpeg 还在运行时，给予更多容忍时间
                            boolean active = streamService.isStreamActive(streamId);
                            if (active) {
                                log.warn("[FLV-{}] FFmpeg进程运行中但文件 {} 秒未增长，继续等待", streamId, stableCountLimit / 10);
                                Thread.sleep(500);
                                continue;
                            }
                            log.warn("[FLV-{}] FFmpeg已停止写入超过 {} 秒，文件大小: {}MB, 已发送: {}MB",
                                    streamId, stableCountLimit / 10,
                                    fileLength / (1024 * 1024), totalBytesSent / (1024 * 1024));
                            break;
                        }

                        // 检测到 FFmpeg 可能已死亡：文件 1 秒未增长且进程不活跃，触发恢复
                        if (stableCount == 50 && System.currentTimeMillis() - lastRecoveryCheck > 10000) {
                            lastRecoveryCheck = System.currentTimeMillis();
                            boolean active = streamService.isStreamActive(streamId);
                            if (!active) {
                                log.warn("[FLV-{}] 检测到FFmpeg进程死亡，触发恢复", streamId);
                                streamService.tryRecoverStream(streamId);

                                // 关闭旧文件（已被tryRecoverStream重命名）
                                try { raf.close(); } catch (IOException ignored) {}
                                raf = null;
                                stableCount = 0;

                                // 等待新FFmpeg创建live.flv，最多5秒
                                Path expectedPath = Paths.get(flvOutputPath, streamId, "live.flv").normalize();
                                for (int wait = 0; wait < 50; wait++) {
                                    File expectedFile = expectedPath.toFile();
                                    if (expectedFile.exists() && expectedFile.length() > 0) {
                                        try {
                                            raf = new RandomAccessFile(expectedFile, "r");
                                            flvFile = expectedFile;
                                            log.info("[FLV-{}] 恢复后切换到新FLV文件: {}", streamId, expectedFile.getAbsolutePath());
                                        } catch (IOException e) {
                                            log.error("[FLV-{}] 打开新FLV文件失败: {}", streamId, e.getMessage());
                                            return;
                                        }
                                        lastFileSize = 0;
                                        noDataCount = 0;
                                        break;
                                    }
                                    try { Thread.sleep(100); } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        return;
                                    }
                                }

                                if (raf == null) {
                                    log.warn("[FLV-{}] 恢复后新FLV文件仍未创建（等待5秒超时），结束传输", streamId);
                                    return;
                                }
                                continue;
                            }
                        }

                        // 检测到 FFmpeg 可能已重启（文件大小不匹配，说明文件被重命名后重建）
                        if (stableCount == fileSwitchThreshold && streamService.isStreamActive(streamId)) {
                            Path expectedPath = Paths.get(flvOutputPath, streamId, "live.flv").normalize();
                            File expectedFile = expectedPath.toFile();
                            if (expectedFile.exists()) {
                                long expectedSize = expectedFile.length();
                                long currentSize = raf.length();
                                if (expectedSize != currentSize) {
                                    log.info("[FLV-{}] 检测到FFmpeg重启，切换读取新FLV文件: {} (旧: {}MB, 新: {}MB)",
                                            streamId, expectedFile.getAbsolutePath(),
                                            currentSize / (1024 * 1024), expectedSize / (1024 * 1024));
                                    try { raf.close(); } catch (IOException ignored) {}
                                    try {
                                        raf = new RandomAccessFile(expectedFile, "r");
                                        flvFile = expectedFile;
                                    } catch (IOException e) {
                                        log.error("[FLV-{}] 打开新FLV文件失败: {}", streamId, e.getMessage());
                                        return;
                                    }
                                    stableCount = 0;
                                    lastFileSize = 0;
                                    continue;
                                }
                            }
                        }
                    } else {
                        stableCount = 0;
                        lastFileSize = fileLength;
                    }

                    noDataCount++;
                    if (noDataCount % 1000 == 0) {
                        log.debug("[FLV-{}] 等待新数据: position={}, fileLength={}",
                                streamId, currentPosition, fileLength);
                    }
                    // 缩短等待时间，从 100ms 改为 20ms，避免播放器缓冲区耗尽导致暂停
                    Thread.sleep(20);
                    continue;
                }

                stableCount = 0;
                int bytesRead = raf.read(buffer);
                if (bytesRead > 0) {
                    out.write(buffer, 0, bytesRead);
                    out.flush();
                    noDataCount = 0;
                    totalBytesSent += bytesRead;

                    long now = System.currentTimeMillis();
                    if (now - lastAccessUpdate > 5000) {
                        streamService.recordStreamAccess(streamId);
                        lastAccessUpdate = now;
                    }
                } else {
                    noDataCount++;
                    // 读到 EOF 但也短暂等待
                    Thread.sleep(20);
                }
            }

            log.info("[FLV-{}] 流传输结束: 已发送 {}MB", streamId, totalBytesSent / (1024 * 1024));

        } catch (IOException e) {
            String msg = e.getMessage();
            if (isClientDisconnect(msg)) {
                log.info("[FLV-{}] 客户端断开连接，停止拉流", streamId);
                streamService.stopStream(streamId);
            } else {
                log.debug("[FLV-{}] 流结束: {}", streamId, msg != null ? msg : e.getClass().getSimpleName());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("[FLV-{}] 流传输被中断", streamId);
        } catch (Exception e) {
            log.debug("[FLV-{}] 流传输异常: {}", streamId, e.getMessage());
        } finally {
            if (raf != null) {
                try { raf.close(); } catch (IOException ignored) {}
            }
        }
    }

    private boolean isClientDisconnect(String msg) {
        if (msg == null) return false;
        return msg.contains("Broken pipe")
                || msg.contains("Connection reset")
                || msg.contains("远程主机")
                || msg.contains("An established connection")
                || msg.contains("Software caused connection abort");
    }

    @GetMapping("/{streamId}/live.flv/head")
    public void getFlvHead(@PathVariable String streamId, HttpServletResponse response) {
        Path flvPath = Paths.get(flvOutputPath, streamId, "live.flv").normalize();
        File flvFile = flvPath.toFile();

        // 流不活跃时不返回旧文件信息
        if (!streamService.isStreamActive(streamId)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");
            try (PrintWriter writer = response.getWriter()) {
                writer.write("{\"exists\":false,\"reason\":\"stream_not_active\"}");
            } catch (IOException e) {
                log.error("[FLV-{}] 获取FLV头信息失败", streamId, e);
            }
            return;
        }

        if (!flvFile.exists()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        response.setHeader("Access-Control-Allow-Origin", "*");

        try (PrintWriter writer = response.getWriter()) {
            writer.write("{\"exists\":true,\"size\":" + flvFile.length() + ",\"lastModified\":" + flvFile.lastModified() + "}");
        } catch (IOException e) {
            log.error("[FLV-{}] 获取FLV头信息失败", streamId, e);
        }
    }
}
