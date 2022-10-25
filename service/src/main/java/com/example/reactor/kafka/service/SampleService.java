package com.example.reactor.kafka.service;

import com.example.reactor.kafka.client.TextClient;
import com.example.reactor.kafka.consumer.SampleReactConsumer;
import com.example.reactor.kafka.dto.KafkaPayload;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class SampleService {

    private final TextClient textClient;
    private final SampleReactConsumer sampleReactConsumer;

    public Mono<String> get(String text) {
        UUID requestId = UUID.randomUUID();
//        UUID requestId = UUID.fromString("68fd8e0d-f1ba-4eae-8ba9-172deee2cc43");
        Flux<KafkaPayload> payloadFlux = sampleReactConsumer.get();
        return payloadFlux.filter(payload -> requestId.equals(payload.getRqId()))
                .map(KafkaPayload::getValue)
                .next()
                .doFirst(() -> textClient.sendToTransform(requestId, text)
                        .subscribe())
                .timeout(Duration.ofSeconds(60));
    }
}
