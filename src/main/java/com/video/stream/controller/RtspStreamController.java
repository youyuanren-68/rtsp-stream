package com.video.stream.controller;

import com.video.stream.service.IRtspStreamService;
import com.video.stream.service.impl.RtspStreamServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Controller
@RequestMapping("/rtspStream")
public class RtspStreamController {
    //rtsp://rtspstream:qrDIMnuNzPL2cwHIg0HRA@zephyr.rtsp.stream/movie
    private static final Logger log = LoggerFactory.getLogger(RtspStreamController.class);
    
    @Autowired
    private IRtspStreamService rtspStreamService;
    
    @Autowired(required = false)
    private RtspStreamServiceImpl rtspStreamServiceImpl;
    
    @Value("${rtsp.hls.access-path:/hls}")
    private String hlsAccessPath;

    @GetMapping("/player")
    public String player() {
        return "player";
    }
    
    @GetMapping("/flv-player")
    public String flvPlayer() {
        return "flv-player";
    }
    
    @PostMapping("/start")
    @ResponseBody
    public Map<String, Object> start(@RequestParam String rtspUrl, 
                           @RequestParam(required = false) String streamId) {
        try {
            if (rtspUrl == null || rtspUrl.trim().isEmpty()) {
                return buildResult(-1, "RTSP地址不能为空", null);
            }
            
            if (!rtspUrl.startsWith("rtsp://")) {
                return buildResult(-1, "RTSP地址格式不正确，必须以rtsp://开头", null);
            }
            
            if (streamId == null || streamId.trim().isEmpty()) {
                streamId = "stream_" + System.currentTimeMillis();
            }
            
            streamId = streamId.trim();
            
            boolean success = rtspStreamService.startStream(rtspUrl, streamId);
            if (success) {
                String m3u8Url = hlsAccessPath + "/" + streamId + "/index.m3u8";
                return buildResult(0, "HLS流已启动", new StartResult(streamId, m3u8Url));
            } else {
                return buildResult(-1, "HLS流启动失败", null);
            }
        } catch (Exception e) {
            log.error("启动RTSP转HLS流失败", e);
            return buildResult(-1, "启动失败: " + e.getMessage(), null);
        }
    }
    
    @PostMapping("/startFlv")
    @ResponseBody
    public Map<String, Object> startFlv(@RequestParam String rtspUrl, 
                              @RequestParam(required = false) String streamId) {
        try {
            if (rtspUrl == null || rtspUrl.trim().isEmpty()) {
                return buildResult(-1, "RTSP地址不能为空", null);
            }
            
            if (!rtspUrl.startsWith("rtsp://")) {
                return buildResult(-1, "RTSP地址格式不正确，必须以rtsp://开头", null);
            }
            
            if (streamId == null || streamId.trim().isEmpty()) {
                streamId = "flv_" + System.currentTimeMillis();
            }
            
            streamId = streamId.trim();
            
            boolean success = rtspStreamService.startFlvStream(rtspUrl, streamId);
            if (success) {
                String flvUrl = "/rtspStream/flv/" + streamId + "/live.flv";
                return buildResult(0, "FLV流已启动", new FlvStartResult(streamId, flvUrl));
            } else {
                return buildResult(-1, "FLV流启动失败", null);
            }
        } catch (Exception e) {
            log.error("启动RTSP转FLV流失败", e);
            return buildResult(-1, "启动失败: " + e.getMessage(), null);
        }
    }
    
    private String getHost() {
        return "localhost";
    }
    
    private int getPort() {
        return 9090;
    }
    
    private static class FlvStartResult {
        private String streamId;
        private String flvUrl;
        
        public FlvStartResult(String streamId, String flvUrl) {
            this.streamId = streamId;
            this.flvUrl = flvUrl;
        }
        
        public String getStreamId() {
            return streamId;
        }
        
        public String getFlvUrl() {
            return flvUrl;
        }
    }
    
    @PostMapping("/stop")
    @ResponseBody
    public Map<String, Object> stop(@RequestParam String streamId) {
        try {
            if (streamId == null || streamId.trim().isEmpty()) {
                return buildResult(-1, "流标识不能为空", null);
            }
            
            rtspStreamService.stopStream(streamId.trim());
            return buildResult(0, "流已停止", null);
        } catch (Exception e) {
            log.error("停止流失败", e);
            return buildResult(-1, "停止失败: " + e.getMessage(), null);
        }
    }
    
    @PostMapping("/stopAll")
    @ResponseBody
    public Map<String, Object> stopAll() {
        try {
            rtspStreamService.stopAllStreams();
            return buildResult(0, "所有流已停止", null);
        } catch (Exception e) {
            log.error("停止所有流失败", e);
            return buildResult(-1, "停止失败: " + e.getMessage(), null);
        }
    }
    
    @GetMapping("/streams")
    @ResponseBody
    public Map<String, Object> getStreams() {
        Map<String, String> streams = rtspStreamService.getActiveStreams();
        return buildResult(0, "success", streams);
    }
    
    @GetMapping("/status")
    @ResponseBody
    public Map<String, Object> checkStatus(@RequestParam String streamId) {
        try {
            if (streamId == null || streamId.trim().isEmpty()) {
                return buildResult(-1, "流标识不能为空", null);
            }
            
            boolean active = rtspStreamService.isStreamActive(streamId.trim());
            Map<String, Object> data = new java.util.HashMap<>();
            data.put("active", active);
            
            if (active && rtspStreamServiceImpl != null) {
                long runningTime = rtspStreamServiceImpl.getStreamRunningTime(streamId.trim());
                data.put("runningTime", runningTime);
                data.put("runningTimeStr", formatRunningTime(runningTime));
                data.put("hlsReady", rtspStreamService.isHlsFileReady(streamId.trim()));
            }
            
            return buildResult(0, active ? "running" : "stopped", data);
        } catch (Exception e) {
            log.error("检查流状态失败", e);
            return buildResult(-1, "检查失败: " + e.getMessage(), null);
        }
    }
    
    @GetMapping("/hlsReady")
    @ResponseBody
    public Map<String, Object> checkHlsReady(@RequestParam String streamId) {
        try {
            if (streamId == null || streamId.trim().isEmpty()) {
                return buildResult(-1, "流标识不能为空", null);
            }
            
            boolean ready = rtspStreamService.isHlsFileReady(streamId.trim());
            Map<String, Object> data = new java.util.HashMap<>();
            data.put("ready", ready);
            
            if (ready) {
                String m3u8Url = hlsAccessPath + "/" + streamId.trim() + "/index.m3u8";
                data.put("m3u8Url", m3u8Url);
            }
            
            return buildResult(0, ready ? "ready" : "not_ready", data);
        } catch (Exception e) {
            log.error("检查HLS文件状态失败", e);
            return buildResult(-1, "检查失败: " + e.getMessage(), null);
        }
    }
    
    @GetMapping("/flvReady")
    @ResponseBody
    public Map<String, Object> checkFlvReady(@RequestParam String streamId) {
        try {
            if (streamId == null || streamId.trim().isEmpty()) {
                return buildResult(-1, "流标识不能为空", null);
            }
            
            boolean ready = rtspStreamService.isFlvFileReady(streamId.trim());
            Map<String, Object> data = new java.util.HashMap<>();
            data.put("ready", ready);
            
            if (ready) {
                String flvUrl = "/rtspStream/flv/" + streamId.trim() + "/live.flv";
                data.put("flvUrl", flvUrl);
            }
            
            return buildResult(0, ready ? "ready" : "not_ready", data);
        } catch (Exception e) {
            log.error("检查FLV文件状态失败", e);
            return buildResult(-1, "检查失败: " + e.getMessage(), null);
        }
    }
    
    private String formatRunningTime(long millis) {
        long seconds = millis / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        
        if (hours > 0) {
            return String.format("%d小时%d分%d秒", hours, minutes % 60, seconds % 60);
        } else if (minutes > 0) {
            return String.format("%d分%d秒", minutes, seconds % 60);
        } else {
            return String.format("%d秒", seconds);
        }
    }
    
    private Map<String, Object> buildResult(int code, String msg, Object data) {
        Map<String, Object> result = new java.util.HashMap<>();
        result.put("code", code);
        result.put("msg", msg);
        result.put("data", data);
        return result;
    }
    
    private static class StartResult {
        private String streamId;
        private String m3u8Url;
        
        public StartResult(String streamId, String m3u8Url) {
            this.streamId = streamId;
            this.m3u8Url = m3u8Url;
        }
        
        public String getStreamId() {
            return streamId;
        }
        
        public String getM3u8Url() {
            return m3u8Url;
        }
    }
}
