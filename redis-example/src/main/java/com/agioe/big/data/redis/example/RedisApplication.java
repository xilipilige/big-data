package com.agioe.big.data.redis.example;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RedisApplication {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(RedisApplication.class);
        application.setBannerMode(Banner.Mode.OFF);
        application.run(args);
    }
}
