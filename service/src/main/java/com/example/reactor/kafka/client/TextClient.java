package com.example.reactor.kafka.client;

import com.example.reactor.kafka.config.property.TextClientProperties;
import lombok.Data;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Component
public class TextClient {
    private final WebClient client;

    public TextClient(TextClientProperties properties) {
        client = WebClient.builder()
                .baseUrl(properties.getUrl())
                .build();
    }

    public Mono<Void> sendToTransform(UUID requestId, String data) {
        Request request = new Request();
        request.setRequestId(requestId);
        request.setData(data);

        return client.post()
                .body(BodyInserters.fromValue(request))
                .retrieve()
                .bodyToMono(Void.class);
    }

    @Data
    public static class Request {
        private UUID requestId;
        private String data;
    }

}
