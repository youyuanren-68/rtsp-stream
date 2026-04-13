package com.video.stream.config;

import com.video.stream.filter.StreamAccessFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FilterConfig {
    
    @Bean
    public FilterRegistrationBean<StreamAccessFilter> streamAccessFilter() {
        FilterRegistrationBean<StreamAccessFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(new StreamAccessFilter());
        registrationBean.addUrlPatterns("/rtspStream/hls/*", "/rtspStream/flv/*");
        registrationBean.setOrder(1);
        return registrationBean;
    }
}
