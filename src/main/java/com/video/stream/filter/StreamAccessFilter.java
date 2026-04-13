package com.video.stream.filter;

import com.video.stream.service.impl.RtspStreamServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

public class StreamAccessFilter implements Filter {
    
    private static final Logger log = LoggerFactory.getLogger(StreamAccessFilter.class);
    
    @Value("${rtsp.hls.access-path:/rtspStream/hls}")
    private String hlsAccessPath;
    
    @Value("${rtsp.flv.access-path:/rtspStream/flv}")
    private String flvAccessPath;
    
    @Autowired(required = false)
    private RtspStreamServiceImpl streamService;
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String uri = httpRequest.getRequestURI();
        
        if (streamService != null) {
            String streamId = extractStreamId(uri, hlsAccessPath);
            if (streamId == null) {
                streamId = extractStreamId(uri, flvAccessPath);
            }
            
            if (streamId != null) {
                streamService.recordStreamAccess(streamId);
            }
        }
        
        chain.doFilter(request, response);
    }
    
    private String extractStreamId(String uri, String basePath) {
        if (uri.startsWith(basePath + "/")) {
            String pathAfterBase = uri.substring(basePath.length() + 1);
            int slashIndex = pathAfterBase.indexOf('/');
            if (slashIndex > 0) {
                return pathAfterBase.substring(0, slashIndex);
            }
        }
        return null;
    }
}
