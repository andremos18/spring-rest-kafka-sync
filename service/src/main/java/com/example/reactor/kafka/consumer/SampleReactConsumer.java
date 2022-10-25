package com.example.reactor.kafka.consumer;

import com.example.reactor.kafka.dto.KafkaPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.concurrent.Queues;

import java.util.Objects;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class SampleReactConsumer implements InitializingBean {

    private final KafkaReceiver<Integer, String> kafkaReceiver;
    final Sinks.Many<KafkaPayload> sink =
            Sinks.many().unicast().onBackpressureBuffer(Queues.<KafkaPayload>one().get());

    @Override
    public void afterPropertiesSet() {
        Flux<ReceiverRecord<Integer, String>> kafkaFlux = kafkaReceiver.receive().log();
        kafkaFlux.concatMap(record -> {
            KafkaPayload kafkaPayload = new KafkaPayload();
            kafkaPayload.setRqId(getRqId(record.headers()));
            kafkaPayload.setValue(record.value());
            sink.tryEmitNext(kafkaPayload);
            return Mono.just(kafkaPayload);
        }).log()
                .subscribe();
    }

    public Flux<KafkaPayload> get() {
        return sink.asFlux();
    }

    private UUID getRqId(Headers headers) {
        Header headerRqId = headers.lastHeader("RQ_ID");
        if (Objects.isNull(headerRqId)) return null;
        String rqIdStr = new String(headerRqId.value());
        return UUID.fromString(rqIdStr);
    }

}
