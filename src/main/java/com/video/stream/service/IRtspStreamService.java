package com.video.stream.service;

import java.io.IOException;
import java.util.Map;

public interface IRtspStreamService {
    
    boolean startStream(String rtspUrl, String streamId) throws IOException;
    
    boolean startFlvStream(String rtspUrl, String streamId) throws IOException;
    
    boolean stopStream(String streamId);
    
    void stopAllStreams();
    
    Map<String, String> getActiveStreams();
    
    boolean isStreamActive(String streamId);
    
    boolean isHlsFileReady(String streamId);
    
    boolean isFlvFileReady(String streamId);

    void recordStreamAccess(String streamId);

    void tryRecoverStream(String streamId);

    long getFlvFileLastModified(String streamId);
}