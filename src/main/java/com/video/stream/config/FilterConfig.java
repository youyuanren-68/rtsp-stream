package com.video.stream.config;

import com.video.stream.filter.StreamAccessFilter;
import com.video.stream.service.impl.RtspStreamServiceImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FilterConfig {
    
    @Value("${rtsp.hls.access-path:/rtspStream/hls}")
    private String hlsAccessPath;
    
    @Value("${rtsp.flv.access-path:/rtspStream/flv}")
    private String flvAccessPath;
    
    @Bean
    public FilterRegistrationBean<StreamAccessFilter> streamAccessFilter(RtspStreamServiceImpl streamService) {
        FilterRegistrationBean<StreamAccessFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(new StreamAccessFilter(hlsAccessPath, flvAccessPath, streamService));
        registrationBean.addUrlPatterns("/rtspStream/hls/*", "/rtspStream/flv/*");
        registrationBean.setOrder(1);
        return registrationBean;
    }
}
