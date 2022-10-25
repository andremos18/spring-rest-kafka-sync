package com.example.reactor.kafka.config.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix="text-client")
public class TextClientProperties {

    private String url;
}
