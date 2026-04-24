package com.video.stream.controller;

import com.video.stream.service.IRtspStreamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/rtspStream/flv")
public class FlvStreamController {

    private static final Logger log = LoggerFactory.getLogger(FlvStreamController.class);
    private static final Pattern STREAM_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]+$");
    private static final int STREAM_BUFFER_SIZE = 32 * 1024;
    private static final int FILE_READY_RETRY_COUNT = 30;
    private static final int RECOVERY_FILE_RETRY_COUNT = 300;
    private static final long FILE_READY_SLEEP_MS = 100L;
    private static final long NO_DATA_SLEEP_MS = 20L;
    private static final long RECOVERY_WAIT_SLEEP_MS = 100L;
    private static final long RECOVERY_RETRY_INTERVAL_MS = 3000L;
    private static final long ACCESS_UPDATE_INTERVAL_MS = 5000L;
    private static final long PROCESS_ALIVE_CHECK_INTERVAL_MS = 5000L;
    private static final long FILE_SWITCH_CHECK_INTERVAL_MS = 500L;

    @Value("${rtsp.flv.output-path:D:/video/flv}")
    private String flvOutputPath;

    @Autowired
    private IRtspStreamService streamService;

    @GetMapping("/{streamId}/live.flv")
    public void streamFlv(@PathVariable String streamId, HttpServletResponse response) {
        if (!isValidStreamId(streamId)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        Path basePath = Paths.get(flvOutputPath).normalize();
        Path flvPath = basePath.resolve(Paths.get(streamId, "live.flv")).normalize();
        if (!flvPath.startsWith(basePath)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        if (!streamService.isStreamManaged(streamId)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        File flvFile = flvPath.toFile();
        boolean streamActive = streamService.isStreamActive(streamId);

        if (flvFile.exists() && flvFile.length() > 0) {
            if (!streamActive) {
                log.debug("[FLV-{}] file exists but stream is inactive, return 404", streamId);
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return;
            }
        } else {
            streamService.tryRecoverStream(streamId);

            for (int i = 0; i < FILE_READY_RETRY_COUNT; i++) {
                if (flvFile.exists() && flvFile.length() > 0) {
                    break;
                }
                if (!streamService.isStreamActive(streamId)) {
                    break;
                }
                try {
                    Thread.sleep(FILE_READY_SLEEP_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            if (!flvFile.exists() || flvFile.length() == 0) {
                log.debug("[FLV-{}] FLV file not ready after initial wait", streamId);
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return;
            }
        }

        response.setContentType("video/x-flv");
        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("Pragma", "no-cache");
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Connection", "keep-alive");
        response.setBufferSize(STREAM_BUFFER_SIZE);

        streamService.recordStreamAccess(streamId);
        streamService.registerViewer(streamId);

        RandomAccessFile raf = null;
        OutputStream out = null;
        try {
            out = response.getOutputStream();
            raf = new RandomAccessFile(flvFile, "r");

            byte[] buffer = new byte[STREAM_BUFFER_SIZE];
            int noDataCount = 0;
            int maxNoDataCount = 36000;
            long lastAccessUpdate = System.currentTimeMillis();
            long totalBytesSent = 0;
            long lastAliveCheck = System.currentTimeMillis();
            long lastFileSwitchCheck = 0L;
            long lastRecoveryAttempt = 0L;
            boolean processDied = false;

            log.info("[FLV-{}] start streaming {}", streamId, flvFile.getAbsolutePath());

            while (noDataCount < maxNoDataCount) {
                if (!streamService.isStreamManaged(streamId)) {
                    log.info("[FLV-{}] stream was manually stopped, close current FLV response", streamId);
                    return;
                }

                long now = System.currentTimeMillis();
                if (now - lastAliveCheck >= PROCESS_ALIVE_CHECK_INTERVAL_MS) {
                    lastAliveCheck = now;
                    if (!streamService.isStreamActive(streamId)) {
                        processDied = true;
                    }
                }

                long fileLength = raf.length();
                long currentPosition = raf.getFilePointer();

                if (currentPosition >= fileLength) {
                    if (processDied) {
                        if (now - lastRecoveryAttempt < RECOVERY_RETRY_INTERVAL_MS) {
                            Thread.sleep(RECOVERY_WAIT_SLEEP_MS);
                            continue;
                        }

                        lastRecoveryAttempt = now;
                        log.warn("[FLV-{}] detected FFmpeg process exit, trying recovery", streamId);
                        streamService.tryRecoverStream(streamId);

                        File recoveredFile = waitForRecoveredFile(streamId, basePath);
                        if (recoveredFile == null) {
                            log.warn("[FLV-{}] recovered FLV file was not created in time, keep current response waiting", streamId);
                            Thread.sleep(RECOVERY_WAIT_SLEEP_MS);
                            continue;
                        }

                        try {
                            try {
                                raf.close();
                            } catch (IOException ignored) {
                            }
                            raf = new RandomAccessFile(recoveredFile, "r");
                            flvFile = recoveredFile;
                            noDataCount = 0;
                            lastAliveCheck = System.currentTimeMillis();
                            lastRecoveryAttempt = 0L;
                            processDied = false;
                            log.info("[FLV-{}] switched to recovered FLV file {}", streamId, recoveredFile.getAbsolutePath());
                        } catch (IOException e) {
                            log.error("[FLV-{}] open recovered FLV file failed: {}", streamId, e.getMessage());
                            Thread.sleep(RECOVERY_WAIT_SLEEP_MS);
                        }
                        continue;
                    }

                    if (now - lastFileSwitchCheck >= FILE_SWITCH_CHECK_INTERVAL_MS) {
                        lastFileSwitchCheck = now;
                        File expectedFile = basePath.resolve(Paths.get(streamId, "live.flv")).normalize().toFile();
                        if (expectedFile.exists()) {
                            long expectedSize = expectedFile.length();
                            long currentSize = raf.length();
                            if (expectedSize != currentSize) {
                                log.info("[FLV-{}] detected FLV file switch, reopen {}", streamId, expectedFile.getAbsolutePath());
                                try {
                                    raf.close();
                                } catch (IOException ignored) {
                                }
                                try {
                                    raf = new RandomAccessFile(expectedFile, "r");
                                    flvFile = expectedFile;
                                    noDataCount = 0;
                                } catch (IOException e) {
                                    log.error("[FLV-{}] reopen new FLV file failed: {}", streamId, e.getMessage());
                                    return;
                                }
                                continue;
                            }
                        }
                    }

                    noDataCount++;
                    if (noDataCount % 1000 == 0) {
                        log.debug("[FLV-{}] waiting for data position={}, fileLength={}", streamId, currentPosition, fileLength);
                    }
                    Thread.sleep(NO_DATA_SLEEP_MS);
                    continue;
                }

                int bytesRead = raf.read(buffer);
                if (bytesRead > 0) {
                    out.write(buffer, 0, bytesRead);
                    out.flush();
                    noDataCount = 0;
                    totalBytesSent += bytesRead;

                    long now2 = System.currentTimeMillis();
                    if (now2 - lastAccessUpdate > ACCESS_UPDATE_INTERVAL_MS) {
                        streamService.recordStreamAccess(streamId);
                        lastAccessUpdate = now2;
                    }
                } else {
                    noDataCount++;
                    Thread.sleep(NO_DATA_SLEEP_MS);
                }
            }

            log.warn("[FLV-{}] stream finished because no data arrived for too long, sent {}MB", streamId, totalBytesSent / (1024 * 1024));
        } catch (IOException e) {
            String msg = e.getMessage();
            if (isClientDisconnect(msg)) {
                log.info("[FLV-{}] client disconnected, keep FFmpeg alive for reuse", streamId);
            } else {
                log.debug("[FLV-{}] stream closed {}", streamId, msg != null ? msg : e.getClass().getSimpleName());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("[FLV-{}] stream interrupted", streamId);
        } catch (Exception e) {
            log.debug("[FLV-{}] stream error {}", streamId, e.getMessage());
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException ignored) {
                }
            }
            streamService.unregisterViewer(streamId);
        }
    }

    @GetMapping("/{streamId}/live.flv/head")
    public void getFlvHead(@PathVariable String streamId, HttpServletResponse response) {
        if (!isValidStreamId(streamId)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        Path basePath = Paths.get(flvOutputPath).normalize();
        Path flvPath = basePath.resolve(Paths.get(streamId, "live.flv")).normalize();
        if (!flvPath.startsWith(basePath)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        File flvFile = flvPath.toFile();

        if (!streamService.isStreamActive(streamId)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");
            try (PrintWriter writer = response.getWriter()) {
                writer.write("{\"exists\":false,\"reason\":\"stream_not_active\"}");
            } catch (IOException e) {
                log.error("[FLV-{}] get FLV head failed", streamId, e);
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
            log.error("[FLV-{}] get FLV head failed", streamId, e);
        }
    }

    private File waitForRecoveredFile(String streamId, Path basePath) throws InterruptedException {
        Path expectedPath = basePath.resolve(Paths.get(streamId, "live.flv")).normalize();
        for (int wait = 0; wait < RECOVERY_FILE_RETRY_COUNT; wait++) {
            if (!streamService.isStreamManaged(streamId)) {
                return null;
            }
            File expectedFile = expectedPath.toFile();
            if (expectedFile.exists() && expectedFile.length() > 0) {
                return expectedFile;
            }
            Thread.sleep(FILE_READY_SLEEP_MS);
        }
        return null;
    }

    private boolean isClientDisconnect(String msg) {
        if (msg == null) {
            return false;
        }
        return msg.contains("Broken pipe")
                || msg.contains("Connection reset")
                || msg.contains("An established connection")
                || msg.contains("Software caused connection abort");
    }

    private boolean isValidStreamId(String streamId) {
        return streamId != null && STREAM_ID_PATTERN.matcher(streamId).matches();
    }
}
