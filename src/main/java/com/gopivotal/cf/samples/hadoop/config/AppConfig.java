package com.gopivotal.cf.samples.hadoop.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.cloudfoundry.CloudFoundryConnector;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableAutoConfiguration
@ComponentScan(basePackages = "com.gopivotal.cf.samples.hadoop")
public class AppConfig {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(AppConfig.class);
        if (new CloudFoundryConnector().isInMatchingCloud()) {
            app.setAdditionalProfiles("cloud");
        }
        app.run(args);
    }
    
}
