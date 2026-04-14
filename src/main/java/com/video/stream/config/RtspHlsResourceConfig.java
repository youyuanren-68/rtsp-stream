package com.video.stream.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class RtspHlsResourceConfig implements WebMvcConfigurer {

    @Autowired
    private HlsAccessInterceptor hlsAccessInterceptor;

    @Value("${rtsp.hls.output-path:D:/video/hls}")
    private String hlsOutputPath;

    @Value("${rtsp.hls.access-path:/hls}")
    private String hlsAccessPath;

    @Value("${rtsp.flv.output-path:D:/video/flv}")
    private String flvOutputPath;

    @Value("${rtsp.flv.access-path:/flv}")
    private String flvAccessPath;

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        String hlsLocation = "file:" + (hlsOutputPath.endsWith("/") ? hlsOutputPath : hlsOutputPath + "/");

        registry.addResourceHandler(hlsAccessPath + "/**")
                .addResourceLocations(hlsLocation)
                .setCachePeriod(0);

        String flvLocation = "file:" + (flvOutputPath.endsWith("/") ? flvOutputPath : flvOutputPath + "/");

        registry.addResourceHandler(flvAccessPath + "/**")
                .addResourceLocations(flvLocation)
                .setCachePeriod(0);

        registry.addResourceHandler("/static/**")
                .addResourceLocations("classpath:/static/")
                .setCachePeriod(0);
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(hlsAccessInterceptor)
                .addPathPatterns("/rtspStream/hls/**");
    }
}
