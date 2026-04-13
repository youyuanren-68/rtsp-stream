package com.video.stream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class RtspStreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(RtspStreamApplication.class, args);
        System.out.println("(♥◠‿◠)ﾉﾞ  RTSP流媒体服务启动成功   ლ(´ڡ`ლ)ﾞ");
    }
}
