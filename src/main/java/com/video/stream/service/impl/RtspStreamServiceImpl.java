package com.video.stream.service.impl;

import com.video.stream.service.IRtspStreamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

@Service
public class RtspStreamServiceImpl implements IRtspStreamService {
    
    private static final Logger log = LoggerFactory.getLogger(RtspStreamServiceImpl.class);
    
    private static final Pattern STREAM_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]+$");
    
    @Value("${rtsp.hls.output-path:D:/video/hls}")
    private String hlsOutputPath;
    
    @Value("${rtsp.flv.output-path:D:/video/flv}")
    private String flvOutputPath;
    
    @Value("${rtsp.ffmpeg.path:D:\\AI\\ffmpeg-8.1-essentials_build\\bin\\ffmpeg.exe}")
    private String ffmpegPath;
    
    @Value("${rtsp.hls.segment-time:1}")
    private int segmentTime;
    
    @Value("${rtsp.hls.list-size:3}")
    private int listSize;
    
    @Value("${rtsp.ffmpeg.preset:ultrafast}")
    private String preset;
    
    @Value("${rtsp.ffmpeg.tune:zerolatency}")
    private String tune;
    
    @Value("${rtsp.ffmpeg.video-codec:libx264}")
    private String videoCodec;
    
    @Value("${rtsp.ffmpeg.audio-codec:aac}")
    private String audioCodec;
    
    @Value("${rtsp.ffmpeg.audio-sample-rate:44100}")
    private int audioSampleRate;
    
    @Value("${rtsp.ffmpeg.rtsp-transport:tcp}")
    private String rtspTransport;
    
    @Value("${rtsp.hls.cleanup-enabled:true}")
    private boolean cleanupEnabled;
    
    @Value("${rtsp.stream.auto-stop-enabled:true}")
    private boolean autoStopEnabled;
    
    @Value("${rtsp.stream.auto-stop-timeout:60000}")
    private long autoStopTimeout;
    
    @Value("${rtsp.flv.cleanup-enabled:true}")
    private boolean flvCleanupEnabled;

    @Value("${rtsp.flv.max-file-size-mb:500}")
    private int flvMaxFileSizeMb;

    @Value("${rtsp.stream.max-concurrent-streams:50}")
    private int maxConcurrentStreams;

    @Value("${rtsp.stream.max-reconnect-attempts:10}")
    private int maxReconnectAttempts;
    
    private final Map<String, Process> activeProcesses = new ConcurrentHashMap<>();
    private final Map<String, String> streamStatus = new ConcurrentHashMap<>();
    private final Map<String, Long> streamStartTime = new ConcurrentHashMap<>();
    private final Map<String, String> streamType = new ConcurrentHashMap<>();
    private final Map<String, Boolean> hlsFileReady = new ConcurrentHashMap<>();
    private final Map<String, Boolean> flvFileReady = new ConcurrentHashMap<>();
    private final Map<String, String> streamRtspUrls = new ConcurrentHashMap<>();
    private final Map<String, Long> streamLastAccessTime = new ConcurrentHashMap<>();

    // 线程追踪：用于在停止流时清理监控线程
    private final Map<String, List<Thread>> streamThreads = new ConcurrentHashMap<>();

    // 重连退避：防止无限重启循环
    private final Map<String, Integer> reconnectAttempts = new ConcurrentHashMap<>();
    private final Map<String, Long> lastReconnectTime = new ConcurrentHashMap<>();
    
    @PostConstruct
    public void init() {
        File outputDir = new File(hlsOutputPath);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
            log.info("创建HLS输出目录: {}", hlsOutputPath);
        }
        
        File flvDir = new File(flvOutputPath);
        if (!flvDir.exists()) {
            flvDir.mkdirs();
            log.info("创建FLV输出目录: {}", flvOutputPath);
        }
        
        File ffmpegFile = new File(ffmpegPath);
        if (!ffmpegFile.exists()) {
            log.warn("FFmpeg可执行文件不存在: {}，请检查配置", ffmpegPath);
        } else {
            log.info("FFmpeg路径: {}", ffmpegPath);
        }
    }
    
    @Override
    public boolean startStream(String rtspUrl, String streamId) throws IOException {
        if (rtspUrl == null || rtspUrl.isEmpty()) {
            log.error("RTSP地址不能为空");
            return false;
        }
        
        if (streamId == null || streamId.isEmpty()) {
            log.error("流标识不能为空");
            return false;
        }
        
        if (!STREAM_ID_PATTERN.matcher(streamId).matches()) {
            log.error("流标识格式不合法，只能包含字母、数字、下划线和连字符: {}", streamId);
            streamStatus.put(streamId, "error: 流标识格式不合法");
            return false;
        }
        
        if (streamId.contains("..") || streamId.contains("/") || streamId.contains("\\")) {
            log.error("流标识包含非法字符，可能存在路径遍历攻击: {}", streamId);
            streamStatus.put(streamId, "error: 流标识包含非法字符");
            return false;
        }
        
        File ffmpegFile = new File(ffmpegPath);
        if (!ffmpegFile.exists()) {
            log.error("FFmpeg可执行文件不存在: {}", ffmpegPath);
            streamStatus.put(streamId, "error: FFmpeg不存在");
            return false;
        }
        
        if (activeProcesses.containsKey(streamId)) {
            Process existingProcess = activeProcesses.get(streamId);
            if (existingProcess != null && existingProcess.isAlive()) {
                log.warn("流 {} 已存在且正在运行，返回现有流信息", streamId);
                return true;
            } else {
                log.warn("流 {} 已存在但未运行，清理旧状态", streamId);
                stopStream(streamId);
            }
        }

        // 检查最大并发流数量
        long activeCount = activeProcesses.values().stream().filter(p -> p != null && p.isAlive()).count();
        if (activeCount >= maxConcurrentStreams) {
            log.error("已达到最大并发流数量限制: {}, 当前活跃流: {}", maxConcurrentStreams, activeCount);
            streamStatus.put(streamId, "error: 超过最大并发流数量");
            return false;
        }

        String streamDir = hlsOutputPath + "/" + streamId;
        Path streamPath = Paths.get(streamDir).normalize();
        Path basePath = Paths.get(hlsOutputPath).normalize();
        
        if (!streamPath.startsWith(basePath)) {
            log.error("检测到路径遍历攻击尝试: streamId={}, streamDir={}", streamId, streamDir);
            streamStatus.put(streamId, "error: 非法路径");
            return false;
        }
        
        File dir = new File(streamDir);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            if (!created) {
                log.error("创建流目录失败: streamId={}, dir={}", streamId, streamDir);
                streamStatus.put(streamId, "error: 创建输出目录失败");
                return false;
            }
            log.info("已创建流目录: {}", streamDir);
        }
        
        if (!dir.canWrite()) {
            log.error("流目录没有写入权限: streamId={}, dir={}", streamId, streamDir);
            streamStatus.put(streamId, "error: 输出目录没有写入权限");
            return false;
        }
        
        String m3u8Path = streamDir + "/index.m3u8";
        
        ProcessBuilder processBuilder = new ProcessBuilder(
            ffmpegPath,
            "-rtsp_transport", rtspTransport,
            "-fflags", "nobuffer",
            "-flags", "low_delay",
            "-i", rtspUrl,
            "-c:v", videoCodec,
            "-preset", preset,
            "-tune", tune,
            "-c:a", audioCodec,
            "-ar", String.valueOf(audioSampleRate),
            "-f", "hls",
            "-hls_time", String.valueOf(segmentTime),
            "-hls_list_size", String.valueOf(listSize),
            "-hls_flags", "delete_segments+program_date_time",
            "-hls_segment_filename", streamDir + "/seg_%05d.ts",
            "-hls_start_number_source", "generic",
            "-hls_allow_cache", "1",
            "-y",
            m3u8Path
        );
        
        processBuilder.redirectErrorStream(true);
        
        try {
            Process process = processBuilder.start();
            activeProcesses.put(streamId, process);
            streamStatus.put(streamId, "running");
            streamStartTime.put(streamId, System.currentTimeMillis());
            streamType.put(streamId, "hls");
            hlsFileReady.put(streamId, false);
            streamRtspUrls.put(streamId, rtspUrl);
            streamLastAccessTime.put(streamId, System.currentTimeMillis());
            
            startLogReader(streamId, process);
            startProcessMonitor(streamId, process);
            startHlsFileMonitor(streamId, m3u8Path);
            
            log.info("RTSP转HLS流已启动: streamId={}, rtspUrl={}, output={}", streamId, rtspUrl, m3u8Path);
            return true;
            
        } catch (IOException e) {
            log.error("启动FFmpeg失败: streamId={}, error={}", streamId, e.getMessage(), e);
            cleanupStreamState(streamId);
            return false;
        }
    }
    
    @Override
    public boolean startFlvStream(String rtspUrl, String streamId) throws IOException {
        if (rtspUrl == null || rtspUrl.isEmpty()) {
            log.error("RTSP地址不能为空");
            return false;
        }
        
        if (streamId == null || streamId.isEmpty()) {
            log.error("流标识不能为空");
            return false;
        }
        
        if (!STREAM_ID_PATTERN.matcher(streamId).matches()) {
            log.error("流标识格式不合法，只能包含字母、数字、下划线和连字符: {}", streamId);
            streamStatus.put(streamId, "error: 流标识格式不合法");
            return false;
        }
        
        if (streamId.contains("..") || streamId.contains("/") || streamId.contains("\\")) {
            log.error("流标识包含非法字符，可能存在路径遍历攻击: {}", streamId);
            streamStatus.put(streamId, "error: 流标识包含非法字符");
            return false;
        }
        
        File ffmpegFile = new File(ffmpegPath);
        if (!ffmpegFile.exists()) {
            log.error("FFmpeg可执行文件不存在: {}", ffmpegPath);
            streamStatus.put(streamId, "error: FFmpeg不存在");
            return false;
        }
        
        if (activeProcesses.containsKey(streamId)) {
            Process existingProcess = activeProcesses.get(streamId);
            String existingType = streamType.get(streamId);
            
            if (existingProcess != null && existingProcess.isAlive()) {
                if ("flv".equals(existingType)) {
                    log.warn("FLV流 {} 已存在且正在运行，返回现有流信息", streamId);
                    return true;
                } else {
                    log.warn("流 {} 已存在但类型为{}，需要先停止", streamId, existingType);
                    stopStream(streamId);
                }
            } else {
                log.warn("FLV流 {} 已存在但未运行，清理旧状态", streamId);
                stopStream(streamId);
            }
        }

        // 检查最大并发流数量
        long activeCount = activeProcesses.values().stream().filter(p -> p != null && p.isAlive()).count();
        if (activeCount >= maxConcurrentStreams) {
            log.error("已达到最大并发流数量限制: {}, 当前活跃流: {}", maxConcurrentStreams, activeCount);
            streamStatus.put(streamId, "error: 超过最大并发流数量");
            return false;
        }

        String streamDir = flvOutputPath + "/" + streamId;
        Path streamPath = Paths.get(streamDir).normalize();
        Path basePath = Paths.get(flvOutputPath).normalize();
        
        if (!streamPath.startsWith(basePath)) {
            log.error("检测到路径遍历攻击尝试: streamId={}, streamDir={}", streamId, streamDir);
            streamStatus.put(streamId, "error: 非法路径");
            return false;
        }
        
        File dir = new File(streamDir);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            if (!created) {
                log.error("创建FLV流目录失败: streamId={}, dir={}", streamId, streamDir);
                streamStatus.put(streamId, "error: 创建输出目录失败");
                return false;
            }
            log.info("已创建FLV流目录: {}", streamDir);
        }
        
        if (!dir.canWrite()) {
            log.error("FLV流目录没有写入权限: streamId={}, dir={}", streamId, streamDir);
            streamStatus.put(streamId, "error: 输出目录没有写入权限");
            return false;
        }
        
        String flvOutput = streamDir + "/live.flv";
        
        log.info("[FFmpeg-{}] 准备启动FLV转码，命令参数: rtspUrl={}, output={}", streamId, rtspUrl, flvOutput);
        
        ProcessBuilder processBuilder = new ProcessBuilder(
            ffmpegPath,
            "-fflags", "nobuffer+genpts",
            "-flags", "low_delay",
            "-rtsp_transport", rtspTransport,
            "-i", rtspUrl,
            "-c:v", videoCodec,
            "-preset", preset,
            "-tune", tune,
            "-c:a", audioCodec,
            "-ar", String.valueOf(audioSampleRate),
            "-f", "flv",
            "-flvflags", "no_duration_filesize",
            flvOutput
        );
        
        processBuilder.redirectErrorStream(true);
        
        try {
            Process process = processBuilder.start();
            activeProcesses.put(streamId, process);
            streamStatus.put(streamId, "running");
            streamStartTime.put(streamId, System.currentTimeMillis());
            streamType.put(streamId, "flv");
            flvFileReady.put(streamId, false);
            streamRtspUrls.put(streamId, rtspUrl);
            streamLastAccessTime.put(streamId, System.currentTimeMillis());
            
            startLogReader(streamId, process);
            startProcessMonitor(streamId, process);
            startFlvFileMonitor(streamId, flvOutput);
            
            log.info("RTSP转FLV流已启动: streamId={}, rtspUrl={}, output={}", streamId, rtspUrl, flvOutput);
            return true;
            
        } catch (IOException e) {
            log.error("启动FFmpeg失败: streamId={}, error={}", streamId, e.getMessage(), e);
            cleanupStreamState(streamId);
            return false;
        }
    }
    
    @Override
    public boolean stopStream(String streamId) {
        if (streamId == null || streamId.isEmpty()) {
            log.error("流标识不能为空");
            return false;
        }

        if (!STREAM_ID_PATTERN.matcher(streamId).matches()) {
            log.error("流标识格式不合法: {}", streamId);
            return false;
        }

        Process process = activeProcesses.get(streamId);
        if (process != null && process.isAlive()) {
            process.destroy();
            try {
                if (!process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    log.warn("FFmpeg进程未正常退出，强制终止: streamId={}", streamId);
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("停止FFmpeg进程被中断: streamId={}", streamId);
            }
            log.info("RTSP转HLS流已停止: streamId={}", streamId);
        }

        // 清理所有监控线程
        cleanupStreamThreads(streamId);

        // 统一清理所有状态
        cleanupStreamState(streamId);
        return true;
    }
    
    @Override
    public void stopAllStreams() {
        for (String streamId : activeProcesses.keySet()) {
            stopStream(streamId);
        }
        log.info("所有RTSP转流已停止");
    }
    
    @Override
    public Map<String, String> getActiveStreams() {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, Process> entry : activeProcesses.entrySet()) {
            String streamId = entry.getKey();
            Process process = entry.getValue();
            boolean isAlive = process != null && process.isAlive();
            String type = streamType.getOrDefault(streamId, "unknown");
            result.put(streamId, isAlive ? "running_" + type : "stopped");
        }
        return result;
    }
    
    @Override
    public boolean isStreamActive(String streamId) {
        Process process = activeProcesses.get(streamId);
        return process != null && process.isAlive();
    }
    
    public long getStreamRunningTime(String streamId) {
        Long startTime = streamStartTime.get(streamId);
        if (startTime == null) {
            return 0;
        }
        return System.currentTimeMillis() - startTime;
    }
    
    /**
     * 统一清理指定 streamId 的所有状态
     */
    private void cleanupStreamState(String streamId) {
        activeProcesses.remove(streamId);
        streamStatus.remove(streamId);
        streamStartTime.remove(streamId);
        streamType.remove(streamId);
        hlsFileReady.remove(streamId);
        flvFileReady.remove(streamId);
        streamRtspUrls.remove(streamId);
        streamLastAccessTime.remove(streamId);
        reconnectAttempts.remove(streamId);
        lastReconnectTime.remove(streamId);
    }

    /**
     * 注册监控线程，便于后续清理
     */
    private void registerStreamThread(String streamId, Thread thread) {
        streamThreads.computeIfAbsent(streamId, k -> Collections.synchronizedList(new ArrayList<>())).add(thread);
    }

    /**
     * 清理指定流的所有监控线程
     */
    private void cleanupStreamThreads(String streamId) {
        List<Thread> threads = streamThreads.remove(streamId);
        if (threads != null && !threads.isEmpty()) {
            log.info("[线程清理] 停止流 {} 的 {} 个监控线程", streamId, threads.size());
            for (Thread thread : threads) {
                if (thread.isAlive()) {
                    thread.interrupt();
                }
            }
            // 短暂等待线程结束
            for (Thread thread : threads) {
                try {
                    thread.join(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("[线程清理] 等待线程结束被中断: {}", thread.getName());
                }
            }
        }
    }

    /**
     * 计算重连退避延迟（指数增长：3s, 6s, 12s, 30s, 60s...）
     */
    private long calculateReconnectDelay(int attempts) {
        long delay = 3000L * (1L << Math.min(attempts, 5)); // 最大 3000*32=96000ms
        return Math.min(delay, 60000L); // 上限 60 秒
    }

    @Scheduled(fixedDelayString = "${rtsp.hls.cleanup-interval:3600000}", initialDelayString = "${rtsp.hls.cleanup-interval:3600000}")
    public void cleanupOldHlsFiles() {
        if (!cleanupEnabled) {
            return;
        }
        
        try {
            File outputDir = new File(hlsOutputPath);
            if (!outputDir.exists()) {
                return;
            }
            
            File[] streamDirs = outputDir.listFiles(File::isDirectory);
            if (streamDirs == null || streamDirs.length == 0) {
                return;
            }
            
            int cleanedCount = 0;
            for (File streamDir : streamDirs) {
                String streamId = streamDir.getName();
                if (!activeProcesses.containsKey(streamId)) {
                    log.info("清理已停止的HLS流目录: {}", streamId);
                    deleteDirectory(streamDir);
                    cleanedCount++;
                }
            }
            
            if (cleanedCount > 0) {
                log.info("HLS目录清理完成，共清理 {} 个已停止的流目录", cleanedCount);
            }
        } catch (Exception e) {
            log.error("HLS目录清理失败", e);
        }
    }
    
    @Scheduled(fixedDelay = 60000, initialDelay = 60000)
    public void checkHlsStreamHealth() {
        try {
            // 遍历当前活跃流（需要复制快照避免 ConcurrentModificationException）
            List<String> streamIds = new ArrayList<>(activeProcesses.keySet());
            for (String streamId : streamIds) {
                Process process = activeProcesses.get(streamId);
                String type = streamType.get(streamId);

                // 检查 HLS 流是否已停止
                if ("hls".equals(type) && (process == null || !process.isAlive())) {
                    if (!hasRecentViewer(streamId)) {
                        log.info("[健康检查] HLS流 {} 已停止且无观众，停止清理", streamId);
                        stopStream(streamId);
                        continue;
                    }
                    String rtspUrl = streamRtspUrls.get(streamId);
                    if (rtspUrl != null) {
                        log.warn("[健康检查] HLS流 {} 已停止，完整清理后重启...", streamId);
                        // 完整清理所有状态（包括线程）
                        cleanupStreamThreads(streamId);
                        cleanupStreamState(streamId);

                        try {
                            if (startStream(rtspUrl, streamId)) {
                                // 健康检查成功重启，重置重连计数器
                                reconnectAttempts.remove(streamId);
                                log.info("[健康检查] HLS流 {} 重启成功", streamId);
                            } else {
                                log.error("[健康检查] HLS流 {} 重启失败", streamId);
                            }
                        } catch (Exception e) {
                            log.error("[健康检查] 重启HLS流 {} 失败: {}", streamId, e.getMessage(), e);
                        }
                    } else {
                        log.warn("[健康检查] HLS流 {} 缺少RTSP地址，停止并清理", streamId);
                        stopStream(streamId);
                    }
                }
            }
        } catch (Exception e) {
            log.error("HLS流健康检查失败", e);
        }
    }
    
    @Scheduled(fixedDelay = 30000, initialDelay = 30000)
    public void checkInactiveStreams() {
        if (!autoStopEnabled) {
            return;
        }
        
        try {
            long now = System.currentTimeMillis();
            List<String> inactiveStreams = new ArrayList<>();
            
            for (Map.Entry<String, Long> entry : streamLastAccessTime.entrySet()) {
                String streamId = entry.getKey();
                Long lastAccess = entry.getValue();
                
                if (now - lastAccess > autoStopTimeout) {
                    Process process = activeProcesses.get(streamId);
                    if (process != null && process.isAlive()) {
                        inactiveStreams.add(streamId);
                    }
                }
            }
            
            for (String streamId : inactiveStreams) {
                long inactiveTime = (now - streamLastAccessTime.get(streamId)) / 1000;
                log.warn("[自动停止] 流 {} 超过{}秒无访问，自动停止", streamId, inactiveTime);
                stopStream(streamId);
            }
        } catch (Exception e) {
            log.error("检查不活跃流失败", e);
        }
    }
    
    @Scheduled(fixedDelayString = "${rtsp.flv.cleanup-interval:3600000}", initialDelayString = "${rtsp.flv.cleanup-interval:3600000}")
    public void cleanupOldFlvFiles() {
        if (!flvCleanupEnabled) {
            return;
        }
        
        try {
            File outputDir = new File(flvOutputPath);
            if (!outputDir.exists()) {
                return;
            }
            
            File[] streamDirs = outputDir.listFiles(File::isDirectory);
            if (streamDirs == null || streamDirs.length == 0) {
                return;
            }
            
            int cleanedCount = 0;
            long totalCleanedSize = 0;
            
            for (File streamDir : streamDirs) {
                String streamId = streamDir.getName();
                if (!activeProcesses.containsKey(streamId)) {
                    long dirSize = getDirectorySize(streamDir);
                    log.info("清理已停止的FLV流目录: {}, 大小: {}MB", streamId, dirSize / (1024 * 1024));
                    deleteDirectory(streamDir);
                    cleanedCount++;
                    totalCleanedSize += dirSize;
                } else {
                    cleanupOldFlvSegments(streamDir);
                }
            }
            
            if (cleanedCount > 0) {
                log.info("FLV目录清理完成，共清理 {} 个已停止的流目录，释放空间: {}MB", 
                        cleanedCount, totalCleanedSize / (1024 * 1024));
            }
        } catch (Exception e) {
            log.error("FLV目录清理失败", e);
        }
    }
    
    @Scheduled(fixedDelay = 60000, initialDelay = 60000)
    public void checkFlvStreamHealth() {
        try {
            List<String> streamIds = new ArrayList<>(activeProcesses.keySet());
            for (String streamId : streamIds) {
                Process process = activeProcesses.get(streamId);
                String type = streamType.get(streamId);

                if ("flv".equals(type) && (process == null || !process.isAlive())) {
                    if (!hasRecentViewer(streamId)) {
                        log.info("[健康检查] FLV流 {} 已停止且无观众，停止清理", streamId);
                        stopStream(streamId);
                        continue;
                    }
                    String rtspUrl = streamRtspUrls.get(streamId);
                    if (rtspUrl != null) {
                        log.warn("[健康检查] FLV流 {} 已停止，完整清理后重启...", streamId);
                        cleanupStreamThreads(streamId);
                        cleanupStreamState(streamId);

                        try {
                            if (startFlvStream(rtspUrl, streamId)) {
                                reconnectAttempts.remove(streamId);
                                log.info("[健康检查] FLV流 {} 重启成功", streamId);
                            } else {
                                log.error("[健康检查] FLV流 {} 重启失败", streamId);
                            }
                        } catch (Exception e) {
                            log.error("[健康检查] 重启FLV流 {} 失败: {}", streamId, e.getMessage(), e);
                        }
                    } else {
                        log.warn("[健康检查] FLV流 {} 缺少RTSP地址，停止并清理", streamId);
                        stopStream(streamId);
                    }
                }
            }
        } catch (Exception e) {
            log.error("FLV流健康检查失败", e);
        }
    }
    
    private void cleanupOldFlvSegments(File streamDir) {
        File[] files = streamDir.listFiles();
        if (files == null || files.length == 0) {
            return;
        }
        
        long now = System.currentTimeMillis();
        long maxAgeMs = 24 * 60 * 60 * 1000L;
        
        for (File file : files) {
            if (file.isFile() && file.getName().endsWith(".flv")) {
                long fileAge = now - file.lastModified();
                if (fileAge > maxAgeMs) {
                    long fileSize = file.length();
                    if (file.delete()) {
                        log.info("清理过期FLV分片文件: {}, 大小: {}MB", 
                                file.getName(), fileSize / (1024 * 1024));
                    }
                }
            }
        }
    }
    
    private long getDirectorySize(File directory) {
        long size = 0;
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        size += file.length();
                    } else {
                        size += getDirectorySize(file);
                    }
                }
            }
        }
        return size;
    }
    
    private boolean deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
        }
        return directory.delete();
    }
    
    private void startLogReader(String streamId, Process process) {
        Thread logThread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    log.debug("[FFmpeg-{}] {}", streamId, line);
                }
            } catch (IOException e) {
                log.error("[FFmpeg-{}] 读取日志失败: {}", streamId, e.getMessage());
            }
        }, "rtsp-hls-log-" + streamId);
        logThread.setDaemon(true);
        registerStreamThread(streamId, logThread);
        logThread.start();
    }
    
    private void startProcessMonitor(String streamId, Process process) {
        // 在创建线程时就捕获 type 和 rtspUrl，避免 waitFor() 后被其他线程删除
        final String capturedType = streamType.get(streamId);
        final String capturedRtspUrl = streamRtspUrls.get(streamId);

        Thread monitorThread = new Thread(() -> {
            try {
                int exitCode = process.waitFor();
                log.warn("[FFmpeg-{}] {}进程已退出，退出码: {}", streamId,
                        capturedType != null ? capturedType.toUpperCase() : "UNKNOWN", exitCode);
                activeProcesses.remove(streamId);
                hlsFileReady.remove(streamId);
                flvFileReady.remove(streamId);

                // 检查是否超过最大重连次数
                int attempts = reconnectAttempts.getOrDefault(streamId, 0);
                if (attempts >= maxReconnectAttempts) {
                    log.error("[FFmpeg-{}] {}流已达到最大重连次数({})，不再重连", streamId,
                            capturedType != null ? capturedType.toUpperCase() : "UNKNOWN", maxReconnectAttempts);
                    streamStatus.put(streamId, "error: max_reconnect_attempts");
                    cleanupStreamThreads(streamId);
                    cleanupStreamState(streamId);
                    return;
                }

                if (capturedRtspUrl != null && hasRecentViewer(streamId)) {
                    // 指数退避延迟
                    long delay = calculateReconnectDelay(attempts);
                    log.info("[FFmpeg-{}] {}流已退出，{}ms后尝试重连(第{}次)", streamId,
                            capturedType != null ? capturedType.toUpperCase() : "UNKNOWN", delay, attempts + 1);
                    streamStatus.put(streamId, "reconnecting...");

                    Thread.sleep(delay);

                    // 重连前再次检查是否有观众，避免无效重连
                    if (!hasRecentViewer(streamId)) {
                        log.info("[FFmpeg-{}] {}流退避期间观众已离开，取消重连", streamId,
                                capturedType != null ? capturedType.toUpperCase() : "UNKNOWN");
                        streamStatus.put(streamId, "stopped: no viewers");
                        cleanupStreamThreads(streamId);
                        cleanupStreamState(streamId);
                        return;
                    }

                    try {
                        // 先清理旧线程和残留状态，再重启
                        cleanupStreamThreads(streamId);
                        cleanupStreamState(streamId);
                        // 恢复必要状态
                        streamRtspUrls.put(streamId, capturedRtspUrl);
                        streamType.put(streamId, capturedType);

                        boolean success;
                        if ("hls".equals(capturedType)) {
                            success = startStream(capturedRtspUrl, streamId);
                        } else if ("flv".equals(capturedType)) {
                            success = startFlvStream(capturedRtspUrl, streamId);
                        } else {
                            log.error("[FFmpeg-{}] 未知流类型: {}", streamId, capturedType);
                            streamStatus.put(streamId, "error: unknown type");
                            return;
                        }

                        if (success) {
                            log.info("[FFmpeg-{}] {}流重连成功", streamId,
                                    capturedType != null ? capturedType.toUpperCase() : "UNKNOWN");
                            // 重连成功，重置计数器
                            reconnectAttempts.remove(streamId);
                            lastReconnectTime.remove(streamId);
                        } else {
                            log.error("[FFmpeg-{}] {}流重连失败", streamId,
                                    capturedType != null ? capturedType.toUpperCase() : "UNKNOWN");
                            reconnectAttempts.put(streamId, attempts + 1);
                            lastReconnectTime.put(streamId, System.currentTimeMillis());
                            streamStatus.put(streamId, "reconnect_failed");
                        }
                    } catch (Exception e) {
                        log.error("[FFmpeg-{}] {}流重连异常: {}", streamId,
                                capturedType != null ? capturedType.toUpperCase() : "UNKNOWN", e.getMessage(), e);
                        reconnectAttempts.put(streamId, attempts + 1);
                        lastReconnectTime.put(streamId, System.currentTimeMillis());
                        streamStatus.put(streamId, "reconnect_error: " + e.getMessage());
                    }
                } else if (capturedRtspUrl != null) {
                    log.info("[FFmpeg-{}] {}流已退出，无活跃观众，不再重连", streamId,
                            capturedType != null ? capturedType.toUpperCase() : "UNKNOWN");
                    streamStatus.put(streamId, "stopped: no viewers");
                } else {
                    log.warn("[FFmpeg-{}] 无法获取RTSP地址，不重连", streamId);
                    streamStatus.put(streamId, "exited with code " + exitCode + ", no rtsp url");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("[FFmpeg-{}] 进程监控被中断", streamId);
            }
        }, "rtsp-process-monitor-" + streamId);
        monitorThread.setDaemon(true);
        registerStreamThread(streamId, monitorThread);
        monitorThread.start();
    }
    
    private void startHlsFileMonitor(String streamId, String m3u8Path) {
        Thread monitorThread = new Thread(() -> {
            int maxWaitTime = 30000;
            int checkInterval = 500;
            int elapsed = 0;
            File m3u8File = new File(m3u8Path);
            File m3u8Dir = m3u8File.getParentFile();
            
            while (elapsed < maxWaitTime) {
                if (m3u8File.exists() && m3u8File.length() > 0) {
                    if (m3u8Dir != null && m3u8Dir.exists()) {
                        File[] tsFiles = m3u8Dir.listFiles((dir, name) -> name.endsWith(".ts"));
                        if (tsFiles != null && tsFiles.length > 0) {
                            hlsFileReady.put(streamId, true);
                            log.info("[FFmpeg-{}] HLS文件已就绪，分片数: {}", streamId, tsFiles.length);
                            return;
                        }
                    }
                }
                
                try {
                    Thread.sleep(checkInterval);
                    elapsed += checkInterval;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("[FFmpeg-{}] HLS文件监控被中断", streamId);
                    return;
                }
            }
            
            log.warn("[FFmpeg-{}] HLS文件生成超时: {}", streamId, m3u8Path);
            hlsFileReady.put(streamId, false);
        }, "rtsp-hls-file-monitor-" + streamId);
        monitorThread.setDaemon(true);
        registerStreamThread(streamId, monitorThread);
        monitorThread.start();
    }
    
    public boolean isHlsFileReady(String streamId) {
        return Boolean.TRUE.equals(hlsFileReady.get(streamId));
    }
    
    public boolean isFlvFileReady(String streamId) {
        return Boolean.TRUE.equals(flvFileReady.get(streamId));
    }
    
    public void recordStreamAccess(String streamId) {
        streamLastAccessTime.put(streamId, System.currentTimeMillis());
    }

    /**
     * 判断该流是否有活跃观众（最近 autoStopTimeout 时间内有请求）
     */
    private boolean hasRecentViewer(String streamId) {
        Long lastAccess = streamLastAccessTime.get(streamId);
        if (lastAccess == null) return false;
        return (System.currentTimeMillis() - lastAccess) < autoStopTimeout;
    }

    @Override
    public long getFlvFileLastModified(String streamId) {
        File flvFile = new File(flvOutputPath, streamId + "/live.flv");
        if (flvFile.exists()) {
            return flvFile.lastModified();
        }
        return 0;
    }

    @Override
    public synchronized void tryRecoverStream(String streamId) {
        String rtspUrl = streamRtspUrls.get(streamId);
        String type = streamType.get(streamId);
        Process process = activeProcesses.get(streamId);

        if (rtspUrl == null) {
            return;
        }

        if (process != null && process.isAlive()) {
            // 进程还活着，但需要检测是否卡死（文件长时间未写入）
            if (!isStreamStuck(streamId, type)) {
                return;
            }
            log.warn("[恢复] 流 {} 进程卡死（文件超过30秒未写入），强制重启", streamId);
        } else {
            log.warn("[恢复] 流 {} 进程已停止，尝试恢复 (type={})", streamId, type);
        }

        // 停止旧进程并清理
        if (process != null && process.isAlive()) {
            process.destroy();
            try {
                if (!process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 清理旧监控线程和状态
        cleanupStreamThreads(streamId);
        cleanupStreamState(streamId);

        // 保留必要信息用于恢复
        streamRtspUrls.put(streamId, rtspUrl);
        if (type != null) {
            streamType.put(streamId, type);
        }

        String outputDir = "flv".equals(type) ? flvOutputPath : hlsOutputPath;
        String streamDir = outputDir + "/" + streamId;

        if ("flv".equals(type)) {
            File oldFlvFile = new File(streamDir, "live.flv");
            if (oldFlvFile.exists() && oldFlvFile.length() > 0) {
                String newName = "live_" + System.currentTimeMillis() + ".flv";
                File newFlvFile = new File(streamDir, newName);
                if (oldFlvFile.renameTo(newFlvFile)) {
                    log.info("[恢复] 旧FLV文件已重命名: {}", newName);
                }
            }
        } else if ("hls".equals(type)) {
            File oldM3u8 = new File(streamDir, "index.m3u8");
            if (oldM3u8.exists()) {
                String newName = "index_" + System.currentTimeMillis() + ".m3u8";
                File newM3u8 = new File(streamDir, newName);
                if (oldM3u8.renameTo(newM3u8)) {
                    log.info("[恢复] 旧HLS播放列表已重命名: {}", newName);
                }
            }
        }

        try {
            boolean success;
            if ("flv".equals(type)) {
                success = startFlvStream(rtspUrl, streamId);
            } else if ("hls".equals(type)) {
                success = startStream(rtspUrl, streamId);
            } else {
                log.error("[恢复] 未知流类型: {}", streamId);
                return;
            }

            if (success) {
                log.info("[恢复] 流 {} 恢复成功", streamId);
            } else {
                log.error("[恢复] 流 {} 恢复失败", streamId);
            }
        } catch (Exception e) {
            log.error("[恢复] 流 {} 恢复异常: {}", streamId, e.getMessage(), e);
        }
    }

    /**
     * 检测流是否卡死：文件超过30秒未写入
     */
    private boolean isStreamStuck(String streamId, String type) {
        String outputDir = "flv".equals(type) ? flvOutputPath : hlsOutputPath;
        String outputFile;
        if ("flv".equals(type)) {
            outputFile = outputDir + "/" + streamId + "/live.flv";
        } else {
            outputFile = outputDir + "/" + streamId + "/index.m3u8";
        }

        File file = new File(outputFile);
        if (!file.exists()) {
            return true;
        }

        long lastModified = file.lastModified();
        long elapsed = System.currentTimeMillis() - lastModified;
        return elapsed > 30000;
    }
    
    private void startFlvFileMonitor(String streamId, String flvPath) {
        Thread monitorThread = new Thread(() -> {
            int maxWaitTime = 30000;
            int checkInterval = 500;
            int elapsed = 0;
            
            while (elapsed < maxWaitTime) {
                File flvFile = new File(flvPath);
                if (flvFile.exists() && flvFile.length() > 0) {
                    flvFileReady.put(streamId, true);
                    log.info("[FFmpeg-{}] FLV文件已就绪: {}", streamId, flvPath);
                    startFlvFileSizeMonitor(streamId, flvFile);
                    return;
                }
                
                try {
                    Thread.sleep(checkInterval);
                    elapsed += checkInterval;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("[FFmpeg-{}] FLV文件监控被中断", streamId);
                    return;
                }
            }
            
            log.warn("[FFmpeg-{}] FLV文件生成超时: {}", streamId, flvPath);
            flvFileReady.put(streamId, false);
        }, "rtsp-flv-file-monitor-" + streamId);
        monitorThread.setDaemon(true);
        registerStreamThread(streamId, monitorThread);
        monitorThread.start();
    }

    private void startFlvFileSizeMonitor(String streamId, File flvFile) {
        Thread monitorThread = new Thread(() -> {
            long maxFileSizeBytes = flvMaxFileSizeMb * 1024L * 1024L;
            int checkInterval = 5000;
            long lastLoggedSizeMb = 0;

            while (activeProcesses.containsKey(streamId)) {
                try {
                    Thread.sleep(checkInterval);

                    if (flvFile.exists()) {
                        long fileSize = flvFile.length();
                        long fileSizeMb = fileSize / (1024 * 1024);

                        if (fileSize >= maxFileSizeBytes) {
                            log.warn("[FFmpeg-{}] FLV文件大小超过限制 ({}MB >= {}MB)，重启FFmpeg进程",
                                    streamId, fileSizeMb, flvMaxFileSizeMb);
                            restartFlvStream(streamId);
                            return;
                        }

                        // 每增加 100MB 记录一次日志，避免重复触发
                        if (fileSizeMb >= lastLoggedSizeMb + 100 && fileSizeMb > 0) {
                            log.info("[FFmpeg-{}] FLV文件大小: {}MB", streamId, fileSizeMb);
                            lastLoggedSizeMb = fileSizeMb;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("[FFmpeg-{}] FLV文件大小监控被中断", streamId);
                    return;
                }
            }
        }, "rtsp-flv-filesize-monitor-" + streamId);
        monitorThread.setDaemon(true);
        registerStreamThread(streamId, monitorThread);
        monitorThread.start();
    }
    
    private void restartFlvStream(String streamId) {
        String rtspUrl = streamRtspUrls.get(streamId);

        Process process = activeProcesses.get(streamId);
        if (process != null && process.isAlive()) {
            process.destroy();
            try {
                if (!process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 清理旧监控线程和状态
        cleanupStreamThreads(streamId);
        cleanupStreamState(streamId);

        // 保留必要信息用于重启
        streamRtspUrls.put(streamId, rtspUrl);
        streamType.put(streamId, "flv");

        String streamDir = flvOutputPath + "/" + streamId;
        String oldFlvPath = streamDir + "/live.flv";
        String newFlvPath = streamDir + "/live_" + System.currentTimeMillis() + ".flv";

        File oldFile = new File(oldFlvPath);
        if (oldFile.exists()) {
            File newFile = new File(newFlvPath);
            if (oldFile.renameTo(newFile)) {
                log.info("[FFmpeg-{}] 旧FLV文件已重命名: {}", streamId, newFlvPath);
            }
        }

        if (rtspUrl != null && !rtspUrl.isEmpty()) {
            log.info("[FFmpeg-{}] 正在重启FLV流...", streamId);
            try {
                Thread.sleep(1000);
                boolean success = startFlvStream(rtspUrl, streamId);
                if (success) {
                    log.info("[FFmpeg-{}] FLV流重启成功", streamId);
                } else {
                    log.error("[FFmpeg-{}] FLV流重启失败", streamId);
                }
            } catch (Exception e) {
                log.error("[FFmpeg-{}] FLV流重启异常: {}", streamId, e.getMessage(), e);
            }
        } else {
            log.error("[FFmpeg-{}] 无法获取RTSP地址，FLV流重启失败", streamId);
            streamStatus.put(streamId, "stopped: file size limit, restart failed");
        }
    }
    
    @PreDestroy
    public void destroy() {
        log.info("应用关闭，正在停止所有RTSP转HLS流...");
        stopAllStreams();
    }
}
