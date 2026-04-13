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
    
    @Value("${rtsp.stream.auto-stop-timeout:300000}")
    private long autoStopTimeout;
    
    @Value("${rtsp.flv.cleanup-enabled:true}")
    private boolean flvCleanupEnabled;
    
    @Value("${rtsp.flv.max-file-size-mb:500}")
    private int flvMaxFileSizeMb;
    
    private final Map<String, Process> activeProcesses = new ConcurrentHashMap<>();
    private final Map<String, String> streamStatus = new ConcurrentHashMap<>();
    private final Map<String, Long> streamStartTime = new ConcurrentHashMap<>();
    private final Map<String, String> streamType = new ConcurrentHashMap<>();
    private final Map<String, Boolean> hlsFileReady = new ConcurrentHashMap<>();
    private final Map<String, Boolean> flvFileReady = new ConcurrentHashMap<>();
    private final Map<String, String> streamRtspUrls = new ConcurrentHashMap<>();
    private final Map<String, Long> streamLastAccessTime = new ConcurrentHashMap<>();
    
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
            "-hls_start_number_source", "generic",
            "-hls_allow_cache", "0",
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
            streamStatus.put(streamId, "error: " + e.getMessage());
            activeProcesses.remove(streamId);
            streamStartTime.remove(streamId);
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
            if (existingProcess != null && existingProcess.isAlive()) {
                log.warn("FLV流 {} 已存在且正在运行，返回现有流信息", streamId);
                return true;
            } else {
                log.warn("FLV流 {} 已存在但未运行，清理旧状态", streamId);
                stopStream(streamId);
            }
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
        
        ProcessBuilder processBuilder = new ProcessBuilder(
            ffmpegPath,
            "-rtsp_transport", rtspTransport,
            "-i", rtspUrl,
            "-c:v", videoCodec,
            "-preset", preset,
            "-tune", tune,
            "-c:a", audioCodec,
            "-ar", String.valueOf(audioSampleRate),
            "-f", "flv",
            "-flvflags", "no_duration_filesize,no_sequence_end",
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
            streamStatus.put(streamId, "error: " + e.getMessage());
            activeProcesses.remove(streamId);
            streamStartTime.remove(streamId);
            streamType.remove(streamId);
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
        
        activeProcesses.remove(streamId);
        streamStatus.remove(streamId);
        streamStartTime.remove(streamId);
        streamType.remove(streamId);
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
            for (Map.Entry<String, Process> entry : activeProcesses.entrySet()) {
                String streamId = entry.getKey();
                Process process = entry.getValue();
                String type = streamType.get(streamId);
                
                if ("hls".equals(type) && (process == null || !process.isAlive())) {
                    String rtspUrl = streamRtspUrls.get(streamId);
                    if (rtspUrl != null) {
                        log.warn("[健康检查] HLS流 {} 已停止，尝试重启...", streamId);
                        activeProcesses.remove(streamId);
                        streamRtspUrls.remove(streamId);
                        
                        try {
                            startStream(rtspUrl, streamId);
                        } catch (Exception e) {
                            log.error("[健康检查] 重启HLS流 {} 失败: {}", streamId, e.getMessage(), e);
                        }
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
            for (Map.Entry<String, Process> entry : activeProcesses.entrySet()) {
                String streamId = entry.getKey();
                Process process = entry.getValue();
                String type = streamType.get(streamId);
                
                if ("flv".equals(type) && (process == null || !process.isAlive())) {
                    String rtspUrl = streamRtspUrls.get(streamId);
                    if (rtspUrl != null) {
                        log.warn("[健康检查] FLV流 {} 已停止，尝试重启...", streamId);
                        activeProcesses.remove(streamId);
                        streamRtspUrls.remove(streamId);
                        
                        try {
                            startFlvStream(rtspUrl, streamId);
                        } catch (Exception e) {
                            log.error("[健康检查] 重启FLV流 {} 失败: {}", streamId, e.getMessage(), e);
                        }
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
        logThread.start();
    }
    
    private void startProcessMonitor(String streamId, Process process) {
        Thread monitorThread = new Thread(() -> {
            try {
                int exitCode = process.waitFor();
                log.warn("[FFmpeg-{}] 进程已退出，退出码: {}", streamId, exitCode);
                activeProcesses.remove(streamId);
                hlsFileReady.remove(streamId);
                flvFileReady.remove(streamId);
                
                String type = streamType.get(streamId);
                String rtspUrl = streamRtspUrls.get(streamId);
                
                if (rtspUrl != null && exitCode != 0) {
                    log.info("[FFmpeg-{}] {}流异常退出，3秒后自动重连...", streamId, type.toUpperCase());
                    streamStatus.put(streamId, "reconnecting...");
                    
                    Thread.sleep(3000);
                    
                    try {
                        boolean success;
                        if ("hls".equals(type)) {
                            success = startStream(rtspUrl, streamId);
                        } else {
                            success = startFlvStream(rtspUrl, streamId);
                        }
                        
                        if (success) {
                            log.info("[FFmpeg-{}] {}流重连成功", streamId, type.toUpperCase());
                        } else {
                            log.error("[FFmpeg-{}] {}流重连失败", streamId, type.toUpperCase());
                            streamStatus.put(streamId, "reconnect_failed");
                        }
                    } catch (Exception e) {
                        log.error("[FFmpeg-{}] {}流重连异常: {}", streamId, type.toUpperCase(), e.getMessage(), e);
                        streamStatus.put(streamId, "reconnect_error: " + e.getMessage());
                    }
                } else {
                    streamStatus.put(streamId, "exited with code " + exitCode);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("[FFmpeg-{}] 进程监控被中断", streamId);
            }
        }, "rtsp-hls-monitor-" + streamId);
        monitorThread.setDaemon(true);
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
        monitorThread.start();
    }
    
    private void startFlvFileSizeMonitor(String streamId, File flvFile) {
        Thread monitorThread = new Thread(() -> {
            long maxFileSizeBytes = flvMaxFileSizeMb * 1024L * 1024L;
            int checkInterval = 5000;
            
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
                        
                        if (fileSizeMb > 100 && fileSizeMb % 100 == 0) {
                            log.info("[FFmpeg-{}] FLV文件大小: {}MB", streamId, fileSizeMb);
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
        
        activeProcesses.remove(streamId);
        flvFileReady.put(streamId, false);
        
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
