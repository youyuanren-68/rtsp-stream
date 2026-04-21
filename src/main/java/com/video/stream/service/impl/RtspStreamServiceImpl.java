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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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

    @Value("${rtsp.hls.delete-threshold:5}")
    private int hlsDeleteThreshold;
    
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

    @Value("${rtsp.stream.stale-output-timeout:45000}")
    private long staleOutputTimeout;

    @Value("${rtsp.flv.history-retention-ms:300000}")
    private long flvHistoryRetentionMs;

    @Value("${rtsp.flv.history-file-limit:3}")
    private int flvHistoryFileLimit;
    
    private final Map<String, Process> activeProcesses = new ConcurrentHashMap<>();
    private final Map<String, String> streamStatus = new ConcurrentHashMap<>();
    private final Map<String, Long> streamStartTime = new ConcurrentHashMap<>();
    private final Map<String, String> streamType = new ConcurrentHashMap<>();
    private final Map<String, Boolean> hlsFileReady = new ConcurrentHashMap<>();
    private final Map<String, Boolean> flvFileReady = new ConcurrentHashMap<>();
    private final Map<String, String> streamRtspUrls = new ConcurrentHashMap<>();
    private final Map<String, Long> streamLastAccessTime = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> streamViewerCounts = new ConcurrentHashMap<>();
    private final Map<String, Long> flvLastObservedSize = new ConcurrentHashMap<>();
    private final Map<String, Long> streamLastOutputTime = new ConcurrentHashMap<>();

    // 线程追踪：用于在停止流时清理监控线程
    private final Map<String, List<Thread>> streamThreads = new ConcurrentHashMap<>();

    // 重连退避：防止无限重启循环
    private final Map<String, Integer> reconnectAttempts = new ConcurrentHashMap<>();
    private final Map<String, Long> lastReconnectTime = new ConcurrentHashMap<>();
    private final Map<String, Long> lastReconnectFailTime = new ConcurrentHashMap<>();
    
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

        cleanupLingeringFfmpegProcesses();
        cleanupOldHlsFiles();
        cleanupOldFlvFiles();
    }

    /**
     * 启动时清理可能残留的旧 FLV 历史文件（live_TIMESTAMP.flv）
     */
    private void cleanupLingeringFfmpegProcesses() {
        String osName = System.getProperty("os.name", "").toLowerCase();
        if (osName.contains("win")) {
            try {
                String command = "Get-CimInstance Win32_Process -Filter \"name='ffmpeg.exe'\" "
                        + "| Where-Object { $_.CommandLine -and $_.CommandLine.Contains('comment=rtsp-stream:') } "
                        + "| ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue; $_.ProcessId }";
                Process process = new ProcessBuilder("powershell", "-NoProfile", "-Command", command)
                        .redirectErrorStream(true)
                        .start();
                int exitCode = process.waitFor();
                log.info("[FFmpeg启动清理] 已扫描并清理残留rtsp-stream FFmpeg进程, exitCode={}", exitCode);
            } catch (Exception e) {
                log.warn("[FFmpeg启动清理] 清理残留FFmpeg进程失败: {}", e.getMessage());
            }
        }

        File flvDir = new File(flvOutputPath);
        if (!flvDir.exists() || !flvDir.isDirectory()) return;
        File[] streamDirs = flvDir.listFiles(File::isDirectory);
        if (streamDirs == null) return;
        int cleaned = 0;
        long totalSize = 0;
        for (File streamDir : streamDirs) {
            File[] files = streamDir.listFiles();
            if (files == null) continue;
            for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".flv") && !file.getName().equals("live.flv")) {
                    long size = file.length();
                    if (file.delete()) { cleaned++; totalSize += size; }
                }
            }
        }
        if (cleaned > 0) {
            log.info("[启动清理] 清理了 {} 个历史FLV文件，释放 {}MB 空间", cleaned, totalSize / (1024 * 1024));
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
                log.warn("[HLS-{}] 流已存在且正在运行，返回现有流信息", streamId);
                return true;
            } else {
                log.warn("[HLS-{}] 流已存在但未运行，清理旧状态", streamId);
                stopStream(streamId);
            }
        }

        // 检查最大并发流数量
        long activeCount = activeProcesses.values().stream().filter(p -> p != null && p.isAlive()).count();
        if (activeCount >= maxConcurrentStreams) {
            log.error("[HLS-{}] 已达到最大并发流数量限制: {}, 当前活跃流: {}", streamId, maxConcurrentStreams, activeCount);
            streamStatus.put(streamId, "error: 超过最大并发流数量");
            return false;
        }

        String streamDir = hlsOutputPath + "/" + streamId;
        Path streamPath = Paths.get(streamDir).normalize();
        Path basePath = Paths.get(hlsOutputPath).normalize();

        if (!streamPath.startsWith(basePath)) {
            log.error("[HLS-{}] 检测到路径遍历攻击尝试: streamDir={}", streamId, streamDir);
            streamStatus.put(streamId, "error: 非法路径");
            return false;
        }

        File dir = new File(streamDir);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            if (!created) {
                log.error("[HLS-{}] 创建流目录失败: dir={}", streamId, streamDir);
                streamStatus.put(streamId, "error: 创建输出目录失败");
                return false;
            }
            log.info("[HLS-{}] 已创建流目录: {}", streamId, streamDir);
        }

        if (!dir.canWrite()) {
            log.error("[HLS-{}] 流目录没有写入权限: dir={}", streamId, streamDir);
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
            "-force_key_frames", "expr:gte(t,n_forced*" + segmentTime + ")",
            "-sc_threshold", "0",
            "-c:a", audioCodec,
            "-ar", String.valueOf(audioSampleRate),
            "-f", "hls",
            "-hls_time", String.valueOf(segmentTime),
            "-hls_list_size", String.valueOf(listSize),
            "-hls_delete_threshold", String.valueOf(Math.max(hlsDeleteThreshold, 1)),
            "-hls_flags", "delete_segments+independent_segments",
            "-hls_segment_filename", streamDir + "/seg_%05d.ts",
            "-metadata", "comment=rtsp-stream:" + streamId,
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
            streamViewerCounts.computeIfAbsent(streamId, key -> new AtomicInteger());
            streamLastOutputTime.put(streamId, System.currentTimeMillis());
            flvLastObservedSize.remove(streamId);
            
            startLogReader(streamId, process);
            startProcessMonitor(streamId, process);
            startHlsFileMonitor(streamId, m3u8Path);
            
            log.info("[HLS-{}] RTSP转HLS流已启动: rtspUrl={}, output={}", streamId, rtspUrl, m3u8Path);
            return true;
            
        } catch (IOException e) {
            log.error("[HLS-{}] 启动FFmpeg失败: error={}", streamId, e.getMessage(), e);
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
                    log.info("[FLV-{}] FLV流已存在且正在运行，返回现有流信息", streamId);
                    return true;
                } else {
                    log.warn("[FLV-{}] 流已存在但类型为{}，需要先停止", streamId, existingType);
                    stopStream(streamId);
                }
            } else {
                log.warn("[FLV-{}] 流已存在但未运行，清理旧状态", streamId);
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
        
        log.info("[FLV-{}] 准备启动FLV转码: rtspUrl={}, output={}", streamId, rtspUrl, flvOutput);

        ProcessBuilder processBuilder = new ProcessBuilder(
            ffmpegPath,
            "-fflags", "nobuffer+genpts",
            "-flags", "low_delay",
            "-rtsp_transport", rtspTransport,
            "-timeout", "30000000",
            "-i", rtspUrl,
            "-c:v", videoCodec,
            "-preset", preset,
            "-tune", tune,
            "-c:a", audioCodec,
            "-ar", String.valueOf(audioSampleRate),
            "-f", "flv",
            "-flvflags", "no_duration_filesize",
            "-metadata", "comment=rtsp-stream:" + streamId,
            "-y",
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
            streamViewerCounts.computeIfAbsent(streamId, key -> new AtomicInteger());
            streamLastOutputTime.put(streamId, System.currentTimeMillis());
            flvLastObservedSize.put(streamId, 0L);
            
            startLogReader(streamId, process);
            startProcessMonitor(streamId, process);
            startFlvFileMonitor(streamId, flvOutput);
            
            log.info("[FLV-{}] RTSP转FLV流已启动: rtspUrl={}, output={}", streamId, rtspUrl, flvOutput);
            return true;
            
        } catch (IOException e) {
            log.error("[FLV-{}] 启动FFmpeg失败: error={}", streamId, e.getMessage(), e);
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
            killProcessTree(process, streamId);
        }

        cleanupStreamThreads(streamId);
        cleanupStreamState(streamId);
        cleanupStreamMetadata(streamId);
        cleanupStreamFiles(streamId);

        // 清理所有监控线程
        cleanupStreamThreads(streamId);

        // 统一清理所有状态
        cleanupStreamState(streamId);

        // 不立即删除磁盘文件，避免和恢复操作产生 race condition
        // 依赖 cleanupFlvHistoryFiles（恢复后立即清理）+ 定时任务（每 2 分钟兜底）

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
     * 注意：不清理 streamRtspUrls、streamType 和 streamLastAccessTime，
     * 保留用于后续自动恢复和观众判断
     */
    private void cleanupStreamState(String streamId) {
        activeProcesses.remove(streamId);
        streamStatus.remove(streamId);
        streamStartTime.remove(streamId);
        hlsFileReady.remove(streamId);
        flvFileReady.remove(streamId);
        reconnectAttempts.remove(streamId);
        lastReconnectTime.remove(streamId);
        lastReconnectFailTime.remove(streamId);
        flvLastObservedSize.remove(streamId);
        streamLastOutputTime.remove(streamId);
        // 保留 streamType、streamRtspUrls 和 streamLastAccessTime
        // - streamType/streamRtspUrls: 恢复时识别流类型和RTSP地址
        // - streamLastAccessTime: 健康检查判断是否有最近观众
        // 下次 startStream/startFlvStream 会自动覆盖这些值
    }

    private void cleanupStreamMetadata(String streamId) {
        streamType.remove(streamId);
        streamRtspUrls.remove(streamId);
        streamLastAccessTime.remove(streamId);
        streamViewerCounts.remove(streamId);
        streamRecoveryLocks.remove(streamId);
    }

    /**
     * 注册监控线程，便于后续清理
     */
    private void registerStreamThread(String streamId, Thread thread) {
        streamThreads.computeIfAbsent(streamId, k -> Collections.synchronizedList(new ArrayList<>())).add(thread);
    }

    /**
     * 清理指定流的所有监控线程（仅interrupt，不阻塞等待）
     */
    private void cleanupStreamThreads(String streamId) {
        List<Thread> threads = streamThreads.remove(streamId);
        if (threads != null && !threads.isEmpty()) {
            Thread currentThread = Thread.currentThread();
            int count = 0;
            for (Thread thread : threads) {
                // 不中断当前线程自身，避免后续逻辑受中断影响
                if (thread.isAlive() && thread != currentThread) {
                    thread.interrupt();
                    count++;
                }
            }
            if (count > 0) {
                log.info("[线程清理] 已发送中断信号给流 {} 的 {} 个监控线程", streamId, count);
            }
            // 不再使用 thread.join 阻塞等待，让线程自然退出
            // 避免因 FFmpeg 进程卡住导致请求线程长时间阻塞
        }
    }

    /**
     * 终止 FFmpeg 进程。
     * 直接 destroyForcibly()，不阻塞等待。
     * Windows 上 TerminateProcess 是即时操作，不会阻塞。
     * 不阻塞调用线程，避免耗尽 Tomcat 线程池。
     */
    private void killProcessTree(Process process, String streamId) {
        if (process == null) {
            return;
        }
        try {
            // destroyForcibly() 在 Windows 上调 TerminateProcess，即时返回
            // 不阻塞等待进程退出，让操作系统自行清理
            if (process.isAlive()) {
                process.destroyForcibly();
                log.info("[进程清理] 已发送终止信号: streamId={}", streamId);
            }
        } catch (Exception e) {
            log.warn("[进程清理] FFmpeg 进程清理异常: streamId={}, error={}", streamId, e.getMessage());
        }
    }

    /**
     * 清理指定流的磁盘文件（HLS目录或FLV目录）。
     * 在 stopStream 时立即调用，不等待定时清理任务。
     */
    private void cleanupStreamFiles(String streamId) {
        // 清理HLS目录
        File hlsDir = new File(hlsOutputPath, streamId);
        if (hlsDir.exists()) {
            long dirSize = getDirectorySize(hlsDir);
            if (deleteDirectory(hlsDir)) {
                log.info("[文件清理] 已删除HLS流目录: {}, 大小: {}MB", streamId, dirSize / (1024 * 1024));
            }
        }

        // 清理FLV目录
        File flvDir = new File(flvOutputPath, streamId);
        if (flvDir.exists()) {
            long dirSize = getDirectorySize(flvDir);
            if (deleteDirectory(flvDir)) {
                log.info("[文件清理] 已删除FLV流目录: {}, 大小: {}MB", streamId, dirSize / (1024 * 1024));
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
                    log.info("[HLS清理] 已停止的流目录: {}", streamId);
                    deleteDirectory(streamDir);
                    cleanedCount++;
                }
            }
            
            if (cleanedCount > 0) {
                log.info("[HLS清理] 目录清理完成，共清理 {} 个已停止的流目录", cleanedCount);
            }
        } catch (Exception e) {
            log.error("[HLS清理] 目录清理失败", e);
        }
    }
    
    @Scheduled(fixedDelay = 60000, initialDelay = 60000)
    public void checkHlsStreamHealth() {
        try {
            // 1. 遍历当前活跃流（需要复制快照避免 ConcurrentModificationException）
            List<String> streamIds = new ArrayList<>(activeProcesses.keySet());
            for (String streamId : streamIds) {
                Process process = activeProcesses.get(streamId);
                String type = streamType.get(streamId);

                // 检查 HLS 流是否已停止
                if ("hls".equals(type) && (process == null || !process.isAlive())) {
                    if (!hasRecentViewer(streamId)) {
                        log.info("[健康检查-HLS] 流 {} 已停止且无观众，停止清理", streamId);
                        stopStream(streamId);
                        continue;
                    }
                    // 检查重连次数，避免无限重启
                    Integer attempts = reconnectAttempts.get(streamId);
                    if (attempts != null && attempts >= maxReconnectAttempts) {
                        log.warn("[健康检查-HLS] 流 {} 已达最大重连次数({})，停止并清理", streamId, maxReconnectAttempts);
                        stopStream(streamId);
                        continue;
                    }
                    // 检查冷却期
                    Long lastFailTime = lastReconnectFailTime.get(streamId);
                    if (lastFailTime != null && System.currentTimeMillis() - lastFailTime < 3000) {
                        continue; // 跳过，等待冷却期结束
                    }
                    String rtspUrl = streamRtspUrls.get(streamId);
                    if (rtspUrl != null) {
                        log.warn("[健康检查-HLS] 流 {} 已停止，清理状态后重启...", streamId);
                        cleanupStreamState(streamId);
                        streamThreads.remove(streamId);

                        try {
                            if (startStream(rtspUrl, streamId)) {
                                reconnectAttempts.remove(streamId);
                                lastReconnectFailTime.remove(streamId);
                                log.info("[健康检查-HLS] 流 {} 重启成功", streamId);
                            } else {
                                reconnectAttempts.merge(streamId, 1, Integer::sum);
                                lastReconnectFailTime.put(streamId, System.currentTimeMillis());
                                log.error("[健康检查-HLS] 流 {} 重启失败", streamId);
                            }
                        } catch (Exception e) {
                            reconnectAttempts.merge(streamId, 1, Integer::sum);
                            lastReconnectFailTime.put(streamId, System.currentTimeMillis());
                            log.error("[健康检查-HLS] 重启流 {} 失败: {}", streamId, e.getMessage(), e);
                        }
                    } else {
                        log.warn("[健康检查-HLS] 流 {} 缺少RTSP地址，停止并清理", streamId);
                        stopStream(streamId);
                    }
                }
            }

            // 2. 扫描已停止但有元数据的HLS流（进程监控器可能已将其从activeProcesses移除）
            for (String streamId : new ArrayList<>(streamType.keySet())) {
                if (!"hls".equals(streamType.get(streamId))) continue;
                if (activeProcesses.containsKey(streamId)) continue; // 已在上面处理

                // 有HLS元数据但进程不活跃，检查是否有最近访问
                if (hasRecentViewer(streamId)) {
                    // 检查重连次数
                    Integer attempts = reconnectAttempts.get(streamId);
                    if (attempts != null && attempts >= maxReconnectAttempts) {
                        log.warn("[健康检查-HLS] 流 {} 进程已移除且达最大重连次数，停止恢复", streamId, maxReconnectAttempts);
                        stopStream(streamId);
                        continue;
                    }
                    // 检查冷却期
                    Long lastFailTime = lastReconnectFailTime.get(streamId);
                    if (lastFailTime != null && System.currentTimeMillis() - lastFailTime < 3000) {
                        continue;
                    }
                    String rtspUrl = streamRtspUrls.get(streamId);
                    if (rtspUrl != null) {
                        log.warn("[健康检查-HLS] 流 {} 进程已移除但有观众，尝试恢复", streamId);
                        try {
                            cleanupStreamThreads(streamId);
                            cleanupStreamState(streamId);
                            if (startStream(rtspUrl, streamId)) {
                                reconnectAttempts.remove(streamId);
                                lastReconnectFailTime.remove(streamId);
                                log.info("[健康检查-HLS] 流 {} 恢复成功", streamId);
                            } else {
                                reconnectAttempts.merge(streamId, 1, Integer::sum);
                                lastReconnectFailTime.put(streamId, System.currentTimeMillis());
                                log.error("[健康检查-HLS] 流 {} 恢复失败", streamId);
                            }
                        } catch (Exception e) {
                            reconnectAttempts.merge(streamId, 1, Integer::sum);
                            lastReconnectFailTime.put(streamId, System.currentTimeMillis());
                            log.error("[健康检查-HLS] 恢复流 {} 失败: {}", streamId, e.getMessage(), e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("[健康检查-HLS] 健康检查失败", e);
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

                if (getViewerCount(streamId) > 0) {
                    continue;
                }
                
                if (now - lastAccess > autoStopTimeout) {
                    Process process = activeProcesses.get(streamId);
                    if (process != null && process.isAlive()) {
                        inactiveStreams.add(streamId);
                    }
                }
            }
            
            for (String streamId : inactiveStreams) {
                long inactiveTime = (now - streamLastAccessTime.get(streamId)) / 1000;
                log.warn("[自动停止] 流 {} 超过 {} 秒无访问，自动停止", streamId, inactiveTime);
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
                    log.info("[FLV清理] 已停止的流目录: {}, 大小: {}MB", streamId, dirSize / (1024 * 1024));
                    deleteDirectory(streamDir);
                    cleanedCount++;
                    totalCleanedSize += dirSize;
                } else {
                    cleanupOldFlvSegments(streamDir);
                }
            }
            
            if (cleanedCount > 0) {
                log.info("[FLV清理] 目录清理完成，共清理 {} 个已停止的流目录，释放空间: {}MB",
                        cleanedCount, totalCleanedSize / (1024 * 1024));
            }
        } catch (Exception e) {
            log.error("[FLV清理] 目录清理失败", e);
        }
    }
    
    @Scheduled(fixedDelay = 60000, initialDelay = 60000)
    public void checkFlvStreamHealth() {
        try {
            // 1. 遍历当前活跃流
            List<String> streamIds = new ArrayList<>(activeProcesses.keySet());
            for (String streamId : streamIds) {
                Process process = activeProcesses.get(streamId);
                String type = streamType.get(streamId);

                if ("flv".equals(type) && process != null && process.isAlive() && hasRecentViewer(streamId) && isFlvOutputStale(streamId)) {
                    log.warn("[健康检查-FLV] 流{} FFmpeg仍存活但输出长时间无增长，重启拉流", streamId);
                    restartFlvStream(streamId);
                    continue;
                }

                if ("flv".equals(type) && (process == null || !process.isAlive())) {
                    if (!hasRecentViewer(streamId)) {
                        log.info("[健康检查-FLV] 流 {} 已停止且无观众，停止清理", streamId);
                        stopStream(streamId);
                        continue;
                    }
                    // 检查重连次数，避免无限重启
                    Integer attempts = reconnectAttempts.get(streamId);
                    if (attempts != null && attempts >= maxReconnectAttempts) {
                        log.warn("[健康检查-FLV] 流 {} 已达最大重连次数({})，停止并清理", streamId, maxReconnectAttempts);
                        stopStream(streamId);
                        continue;
                    }
                    // 检查冷却期
                    Long lastFailTime = lastReconnectFailTime.get(streamId);
                    if (lastFailTime != null && System.currentTimeMillis() - lastFailTime < 3000) {
                        continue;
                    }
                    String rtspUrl = streamRtspUrls.get(streamId);
                    if (rtspUrl != null) {
                        log.warn("[健康检查-FLV] 流 {} 已停止，清理状态后重启...", streamId);
                        cleanupStreamState(streamId);
                        streamThreads.remove(streamId);

                        try {
                            if (startFlvStream(rtspUrl, streamId)) {
                                reconnectAttempts.remove(streamId);
                                lastReconnectFailTime.remove(streamId);
                                log.info("[健康检查-FLV] 流 {} 重启成功", streamId);
                            } else {
                                reconnectAttempts.merge(streamId, 1, Integer::sum);
                                lastReconnectFailTime.put(streamId, System.currentTimeMillis());
                                log.error("[健康检查-FLV] 流 {} 重启失败", streamId);
                            }
                        } catch (Exception e) {
                            reconnectAttempts.merge(streamId, 1, Integer::sum);
                            lastReconnectFailTime.put(streamId, System.currentTimeMillis());
                            log.error("[健康检查-FLV] 重启流 {} 失败: {}", streamId, e.getMessage(), e);
                        }
                    } else {
                        log.warn("[健康检查-FLV] 流 {} 缺少RTSP地址，停止并清理", streamId);
                        stopStream(streamId);
                    }
                }
            }

            // 2. 扫描已停止但有元数据的FLV流
            for (String streamId : new ArrayList<>(streamType.keySet())) {
                if (!"flv".equals(streamType.get(streamId))) continue;
                if (activeProcesses.containsKey(streamId)) continue;

                if (hasRecentViewer(streamId)) {
                    // 检查重连次数
                    Integer attempts = reconnectAttempts.get(streamId);
                    if (attempts != null && attempts >= maxReconnectAttempts) {
                        log.warn("[健康检查-FLV] 流 {} 进程已移除且达最大重连次数，停止恢复", streamId, maxReconnectAttempts);
                        stopStream(streamId);
                        continue;
                    }
                    // 检查冷却期
                    Long lastFailTime = lastReconnectFailTime.get(streamId);
                    if (lastFailTime != null && System.currentTimeMillis() - lastFailTime < 3000) {
                        continue;
                    }
                    String rtspUrl = streamRtspUrls.get(streamId);
                    if (rtspUrl != null) {
                        log.warn("[健康检查-FLV] 流 {} 进程已移除但有观众，尝试恢复", streamId);
                        try {
                            cleanupStreamThreads(streamId);
                            cleanupStreamState(streamId);
                            if (startFlvStream(rtspUrl, streamId)) {
                                reconnectAttempts.remove(streamId);
                                lastReconnectFailTime.remove(streamId);
                                log.info("[健康检查-FLV] 流 {} 恢复成功", streamId);
                            } else {
                                reconnectAttempts.merge(streamId, 1, Integer::sum);
                                lastReconnectFailTime.put(streamId, System.currentTimeMillis());
                                log.error("[健康检查-FLV] 流 {} 恢复失败", streamId);
                            }
                        } catch (Exception e) {
                            reconnectAttempts.merge(streamId, 1, Integer::sum);
                            lastReconnectFailTime.put(streamId, System.currentTimeMillis());
                            log.error("[健康检查-FLV] 恢复流 {} 失败: {}", streamId, e.getMessage(), e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("[健康检查-FLV] 健康检查失败", e);
        }
    }
    
    private boolean isFlvOutputStale(String streamId) {
        File flvFile = new File(flvOutputPath, streamId + "/live.flv");
        long now = System.currentTimeMillis();
        if (!flvFile.exists() || flvFile.length() <= 0) {
            Long lastOutput = streamLastOutputTime.get(streamId);
            return lastOutput != null && now - lastOutput > staleOutputTimeout;
        }

        long size = flvFile.length();
        Long lastSize = flvLastObservedSize.get(streamId);
        if (lastSize == null || size > lastSize) {
            flvLastObservedSize.put(streamId, size);
            streamLastOutputTime.put(streamId, now);
            return false;
        }

        Long lastOutput = streamLastOutputTime.get(streamId);
        if (lastOutput == null) {
            streamLastOutputTime.put(streamId, Math.max(flvFile.lastModified(), now));
            return false;
        }
        return now - lastOutput > staleOutputTimeout;
    }

    private void cleanupOldFlvSegments(File streamDir) {
        File[] files = streamDir.listFiles();
        if (files == null || files.length == 0) {
            return;
        }

        long now = System.currentTimeMillis();
        long maxAgeMs = Math.max(flvHistoryRetentionMs, 30000L);

        for (File file : files) {
            if (file.isFile() && file.getName().endsWith(".flv") && !file.getName().equals("live.flv")) {
                long fileAge = now - file.lastModified();
                if (fileAge > maxAgeMs) {
                    long fileSize = file.length();
                    if (file.delete()) {
                        log.info("[FLV清理] 清理过期FLV分片文件: {}, 大小: {}MB",
                                file.getName(), fileSize / (1024 * 1024));
                    }
                }
            }
        }
    }

    /**
     * 清理指定 streamId 目录下的历史 FLV 文件（live_TIMESTAMP.flv），
     * 只保留 live.flv（当前 FFmpeg 正在写入的文件）。
     * 在 tryRecoverStream 后立即调用，避免文件堆积。
     */
    private void cleanupFlvHistoryFiles(String streamId) {
        File streamDir = new File(flvOutputPath, streamId);
        if (!streamDir.exists() || !streamDir.isDirectory()) {
            return;
        }

        File[] files = streamDir.listFiles();
        if (files == null || files.length == 0) {
            return;
        }

        int cleaned = 0;
        long totalSize = 0;
        for (File file : files) {
            if (file.isFile()
                    && file.getName().endsWith(".flv")
                    && !file.getName().equals("live.flv")
                    && System.currentTimeMillis() - file.lastModified() > Math.max(flvHistoryRetentionMs, 30000L)) {
                long fileSize = file.length();
                if (file.delete()) {
                    cleaned++;
                    totalSize += fileSize;
                }
            }
        }

        if (cleaned > 0) {
            log.info("[FLV清理] 清理流 {} 的历史FLV文件: {} 个, 释放空间: {}MB",
                    streamId, cleaned, totalSize / (1024 * 1024));
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
        String type = streamType.getOrDefault(streamId, "log");
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
        }, "rtsp-" + type + "-log-" + streamId);
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
                log.warn("[进程监控-{}] {} 进程已退出，退出码: {}", streamId,
                        capturedType != null ? capturedType.toUpperCase() : "UNKNOWN", exitCode);
                activeProcesses.remove(streamId);
                hlsFileReady.remove(streamId);
                flvFileReady.remove(streamId);
                streamStatus.put(streamId, "exited with code " + exitCode);
                log.info("[进程监控-{}] {} 状态已标记为exited，等待拦截器或健康检查恢复", streamId,
                        capturedType != null ? capturedType.toUpperCase() : "UNKNOWN");
                // 进程监控不再主动重连，交由拦截器或健康检查统一处理
                // 避免与拦截器的tryRecoverStream和健康检查冲突
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("[进程监控-{}] 监控线程被中断", streamId);
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
                            log.info("[文件监控-HLS-{}] HLS文件已就绪，分片数: {}", streamId, tsFiles.length);
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
            
            log.warn("[文件监控-HLS-{}] HLS文件生成超时: {}", streamId, m3u8Path);
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

    @Override
    public void registerViewer(String streamId) {
        AtomicInteger count = streamViewerCounts.computeIfAbsent(streamId, key -> new AtomicInteger());
        count.incrementAndGet();
        streamLastAccessTime.put(streamId, System.currentTimeMillis());
    }

    @Override
    public void unregisterViewer(String streamId) {
        AtomicInteger count = streamViewerCounts.get(streamId);
        if (count != null) {
            int remaining = count.decrementAndGet();
            if (remaining <= 0) {
                streamViewerCounts.remove(streamId);
            }
        }
    }

    @Override
    public int getViewerCount(String streamId) {
        AtomicInteger count = streamViewerCounts.get(streamId);
        return count != null ? count.get() : 0;
    }

    /**
     * 判断该流是否有活跃观众（最近 autoStopTimeout 时间内有请求）
     */
    private boolean hasRecentViewer(String streamId) {
        if (getViewerCount(streamId) > 0) {
            return true;
        }
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

    private final Map<String, Object> streamRecoveryLocks = new ConcurrentHashMap<>();

    @Override
    public void tryRecoverStream(String streamId) {
        // per-stream 锁，避免不同流的恢复互相阻塞
        Object lock = streamRecoveryLocks.computeIfAbsent(streamId, k -> new Object());
        synchronized (lock) {
            doRecoverStream(streamId);
        }
        streamRecoveryLocks.remove(streamId);
    }

    private void doRecoverStream(String streamId) {
        String rtspUrl = streamRtspUrls.get(streamId);
        String type = streamType.get(streamId);
        Process process = activeProcesses.get(streamId);
        Long lastAccess = streamLastAccessTime.get(streamId);

        if (rtspUrl == null) {
            // RTSP地址丢失说明该流从未被正确启动或服务器重启后状态丢失
            // 如果有最近访问记录，说明有用户在等待，记录详细日志便于排查
            boolean hasViewer = lastAccess != null && (System.currentTimeMillis() - lastAccess) < autoStopTimeout;
            log.warn("[恢复-{}] 缺少RTSP地址，无法恢复 | type={}, process={}, hasViewer={}, lastAccess={}ms前",
                    streamId, type, (process != null && process.isAlive()) ? "alive" : "dead",
                    hasViewer, lastAccess != null ? (System.currentTimeMillis() - lastAccess) : -1);
            return;
        }

        if (type == null) {
            log.warn("[恢复] 流 {} 元数据丢失(type=null)，无法恢复", streamId);
            return;
        }

        // 检查重连次数，避免无限重试浪费资源
        Integer attempts = reconnectAttempts.get(streamId);
        if (attempts != null && attempts >= maxReconnectAttempts) {
            log.warn("[恢复] 流 {} 已达最大重连次数({})，跳过恢复", streamId, maxReconnectAttempts);
            return;
        }

        // 检查重连冷却期：如果上次恢复失败，至少等待 3 秒再尝试
        Long lastFailTime = lastReconnectFailTime.get(streamId);
        if (lastFailTime != null && System.currentTimeMillis() - lastFailTime < 3000) {
            log.debug("[恢复-{}-{}] 流处于恢复冷却期（3秒内失败过），跳过", streamId, type);
            return;
        }

        // 如果 FFmpeg 进程正在运行，不干预（file.lastModified 不可靠，缓冲区写入会导致误判）
        // 流卡死的情况由定时健康检查（60秒间隔）统一处理，避免每次请求都触发误杀
        if (process != null && process.isAlive()) {
            return;
        }

        log.warn("[恢复-{}-{}] 流已停止，尝试恢复", streamId, type);

        // 清理旧监控线程
        cleanupStreamThreads(streamId);

        // 清理状态（保留streamRtspUrls、streamType、streamLastAccessTime）
        activeProcesses.remove(streamId);
        streamStatus.remove(streamId);
        streamStartTime.remove(streamId);
        hlsFileReady.remove(streamId);
        flvFileReady.remove(streamId);
        reconnectAttempts.remove(streamId);
        lastReconnectTime.remove(streamId);

        String outputDir = "flv".equals(type) ? flvOutputPath : hlsOutputPath;
        String streamDir = outputDir + "/" + streamId;

        if ("flv".equals(type)) {
            File oldFlvFile = new File(streamDir, "live.flv");
            if (oldFlvFile.exists() && oldFlvFile.length() > 0) {
                String newName = "live_" + System.currentTimeMillis() + ".flv";
                File newFlvFile = new File(streamDir, newName);
                if (oldFlvFile.renameTo(newFlvFile)) {
                    log.info("[恢复-FLV-{}] 旧FLV文件已重命名: {}", streamId, newName);
                } else {
                    // rename失败，直接删除旧文件
                    if (oldFlvFile.delete()) {
                        log.info("[恢复-FLV-{}] 旧FLV文件已删除: {}", streamId, oldFlvFile.getAbsolutePath());
                    }
                }
            }
        } else if ("hls".equals(type)) {
            File oldM3u8 = new File(streamDir, "index.m3u8");
            if (oldM3u8.exists()) {
                String newName = "index_" + System.currentTimeMillis() + ".m3u8";
                File newM3u8 = new File(streamDir, newName);
                if (oldM3u8.renameTo(newM3u8)) {
                    log.info("[恢复-HLS-{}] 旧HLS播放列表已重命名: {}", streamId, newName);
                }
            }
        }

        try {
            log.info("[恢复-{}-{}] 正在调用 startFlvStream/startStream", type, streamId);
            boolean success;
            if ("flv".equals(type)) {
                success = startFlvStream(rtspUrl, streamId);
            } else if ("hls".equals(type)) {
                success = startStream(rtspUrl, streamId);
            } else {
                log.error("[恢复] 未知流类型: {}", streamId);
                return;
            }
            log.info("[恢复-{}-{}] startFlvStream/startStream 返回结果: success={}, processAlive={}",
                    type, streamId, success, isStreamActive(streamId));

            if (success) {
                log.info("[恢复-{}-{}] 恢复成功", type, streamId);
                reconnectAttempts.remove(streamId);
                lastReconnectFailTime.remove(streamId);
                // FLV流恢复后清理历史FLV文件
                if ("flv".equals(type)) {
                    cleanupFlvHistoryFiles(streamId);
                }
            } else {
                reconnectAttempts.merge(streamId, 1, Integer::sum);
                lastReconnectFailTime.put(streamId, System.currentTimeMillis());
                log.error("[恢复-{}-{}] 恢复失败", type, streamId);
            }
        } catch (Exception e) {
            reconnectAttempts.merge(streamId, 1, Integer::sum);
            lastReconnectFailTime.put(streamId, System.currentTimeMillis());
            log.error("[恢复-{}-{}] 恢复异常: {}", type, streamId, e.getMessage(), e);
        }
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
                    log.info("[文件监控-FLV-{}] FLV文件已就绪: {}", streamId, flvPath);
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
            
            log.warn("[文件监控-FLV-{}] FLV文件生成超时: {}", streamId, flvPath);
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
                            log.warn("[FLV大小监控-{}] FLV文件大小超过限制 ({}MB >= {}MB)，重启FFmpeg进程",
                                    streamId, fileSizeMb, flvMaxFileSizeMb);
                            restartFlvStream(streamId);
                            return;
                        }

                        // 每增加 100MB 记录一次日志，避免重复触发
                        if (fileSizeMb >= lastLoggedSizeMb + 100 && fileSizeMb > 0) {
                            log.info("[FLV大小监控-{}] FLV文件大小: {}MB", streamId, fileSizeMb);
                            lastLoggedSizeMb = fileSizeMb;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("[FLV大小监控-{}] 文件大小监控线程被中断", streamId);
                    return;
                }
            }
        }, "rtsp-flv-filesize-monitor-" + streamId);
        monitorThread.setDaemon(true);
        registerStreamThread(streamId, monitorThread);
        monitorThread.start();
    }

    /**
     * 重启FLV流：当文件过大时重启FFmpeg。
     * 注意：重启期间会有短暂中断（5-20秒），控制器会自动切换到新文件。
     */
    private void restartFlvStream(String streamId) {
        String rtspUrl = streamRtspUrls.get(streamId);

        Process process = activeProcesses.get(streamId);
        if (process != null && process.isAlive()) {
            killProcessTree(process, streamId);
        }

        // 清理旧监控线程和状态
        cleanupStreamThreads(streamId);
        cleanupStreamState(streamId);

        // 保留必要信息用于重启
        cleanupStreamThreads(streamId);
        streamRtspUrls.put(streamId, rtspUrl);
        streamType.put(streamId, "flv");

        String streamDir = flvOutputPath + "/" + streamId;
        String oldFlvPath = streamDir + "/live.flv";

        // 旧文件重命名，给新FFmpeg腾出空间
        File oldFile = new File(oldFlvPath);
        if (oldFile.exists()) {
            String newFlvPath = streamDir + "/live_" + System.currentTimeMillis() + ".flv";
            File newFile = new File(newFlvPath);
            if (oldFile.renameTo(newFile)) {
                log.info("[FLV重启-{}] 旧FLV文件已重命名: {}", streamId, newFlvPath);
            }
        }

        if (rtspUrl != null && !rtspUrl.isEmpty()) {
            log.info("[FLV重启-{}] 正在重启FLV流...", streamId);
            try {
                Thread.sleep(1000);
                boolean success = startFlvStream(rtspUrl, streamId);
                if (success) {
                    log.info("[FLV重启-{}] FLV流重启成功", streamId);
                    reconnectAttempts.remove(streamId);
                    // 新流启动后清理所有历史FLV文件（live_TIMESTAMP.flv）
                    cleanupFlvHistoryFiles(streamId);
                } else {
                    reconnectAttempts.merge(streamId, 1, Integer::sum);
                    log.error("[FLV重启-{}] FLV流重启失败", streamId);
                }
            } catch (Exception e) {
                reconnectAttempts.merge(streamId, 1, Integer::sum);
                log.error("[FLV重启-{}] FLV流重启异常: {}", streamId, e.getMessage(), e);
            }
        } else {
            log.error("[FLV重启-{}] 无法获取RTSP地址，FLV流重启失败", streamId);
            streamStatus.put(streamId, "stopped: file size limit, restart failed");
        }
    }
    
    @PreDestroy
    public void destroy() {
        log.info("应用关闭，正在停止所有RTSP转流...");
        stopAllStreams();
    }
}
