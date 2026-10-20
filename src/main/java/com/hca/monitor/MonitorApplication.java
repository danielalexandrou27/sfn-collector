package com.hca.monitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class MonitorApplication {
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(MonitorApplication.class);
        // Spring Boot runs "headless" by default, which disables desktop notifications. On Windows keep the
        // desktop available; on servers (Linux/EC2) stay headless.
        app.setHeadless(!System.getProperty("os.name", "").toLowerCase().startsWith("windows"));
        app.run(args);
    }
}
