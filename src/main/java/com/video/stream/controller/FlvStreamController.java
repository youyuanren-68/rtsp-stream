package com.video.stream.controller;

import com.video.stream.service.impl.RtspStreamServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

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
    
    @Autowired(required = false)
    private RtspStreamServiceImpl streamService;
    
    @GetMapping("/{streamId}/live.flv")
    public StreamingResponseBody streamFlv(
            @PathVariable String streamId,
            HttpServletRequest request,
            HttpServletResponse response) {
        
        Path flvPath = Paths.get(flvOutputPath, streamId, "live.flv").normalize();
        File flvFile = flvPath.toFile();
        
        if (!flvFile.exists()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            log.warn("FLV文件不存在: streamId={}", streamId);
            return outputStream -> {};
        }
        
        response.setContentType("video/x-flv");
        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("Pragma", "no-cache");
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Connection", "keep-alive");
        response.setHeader("Transfer-Encoding", "chunked");
        
        if (streamService != null) {
            streamService.recordStreamAccess(streamId);
        }
        
        return outputStream -> {
            RandomAccessFile raf = null;
            try {
                raf = new RandomAccessFile(flvFile, "r");
                byte[] buffer = new byte[8192];
                long lastPosition = 0;
                int noDataCount = 0;
                int maxNoDataCount = 600;
                
                while (noDataCount < maxNoDataCount) {
                    long fileLength = raf.length();
                    long currentPosition = raf.getFilePointer();
                    
                    if (currentPosition >= fileLength) {
                        noDataCount++;
                        Thread.sleep(100);
                        continue;
                    }
                    
                    int bytesRead = raf.read(buffer);
                    if (bytesRead > 0) {
                        outputStream.write(buffer, 0, bytesRead);
                        outputStream.flush();
                        noDataCount = 0;
                        lastPosition = raf.getFilePointer();
                    } else {
                        noDataCount++;
                        Thread.sleep(50);
                    }
                }
                
                log.info("FLV流传输结束: streamId={}, finalPosition={}", streamId, raf.getFilePointer());
                
            } catch (IOException e) {
                if (!e.getMessage().contains("Broken pipe") && !e.getMessage().contains("Connection reset")) {
                    log.error("FLV流传输IO异常: streamId={}", streamId, e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug("FLV流传输被中断: streamId={}", streamId);
            } finally {
                if (raf != null) {
                    try {
                        raf.close();
                    } catch (IOException e) {
                        log.debug("关闭RandomAccessFile失败", e);
                    }
                }
            }
        };
    }
    
    @GetMapping("/{streamId}/live.flv/head")
    public void getFlvHead(@PathVariable String streamId, HttpServletResponse response) {
        Path flvPath = Paths.get(flvOutputPath, streamId, "live.flv").normalize();
        File flvFile = flvPath.toFile();
        
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
            e.printStackTrace();
        }
    }
}
