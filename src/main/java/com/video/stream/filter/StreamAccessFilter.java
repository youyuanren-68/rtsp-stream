package com.video.stream.filter;

import com.video.stream.service.impl.RtspStreamServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

public class StreamAccessFilter implements Filter {
    
    private static final Logger log = LoggerFactory.getLogger(StreamAccessFilter.class);
    
    private final String hlsAccessPath;
    private final String flvAccessPath;
    private final RtspStreamServiceImpl streamService;
    
    public StreamAccessFilter(String hlsAccessPath, String flvAccessPath, RtspStreamServiceImpl streamService) {
        this.hlsAccessPath = hlsAccessPath;
        this.flvAccessPath = flvAccessPath;
        this.streamService = streamService;
    }
    
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
