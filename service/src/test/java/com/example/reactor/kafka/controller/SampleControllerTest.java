package com.example.reactor.kafka.controller;

import com.example.reactor.kafka.client.TextClient;
import com.example.reactor.kafka.dto.KafkaPayload;
import com.example.reactor.kafka.dto.Request;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.web.reactive.function.BodyInserters;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.request;

@SpringBootTest
@TestPropertySource(properties = {
        "text-client.url: ${mockServerURL}",
        "kafka.bootstrapServerConfig: ${spring.embedded.kafka.brokers}"
})
@EmbeddedKafka(partitions = 1)
@AutoConfigureWebTestClient
class SampleControllerTest {

    @Autowired
    protected WebTestClient testClient;

    @Value("${spring.embedded.kafka.brokers}")
    String brokerAddresses;

    protected ObjectMapper mapper = JsonMapper.builder()
            .build();

    public KafkaSender<Integer, String> kafkaSender;

    @Autowired
    protected EmbeddedKafkaBroker embeddedKafkaBroker;

    public static final WireMockServer mockServer = new WireMockServer(wireMockConfig().dynamicPort());

    @BeforeEach
    void setUp() {
        kafkaSender = KafkaSender.create(SenderOptions.create(kafkaProps()));
    }

    @BeforeAll
    static void init() {
        mockServer.start();
    }

    @AfterAll
    static void destroy() {
        mockServer.stop();
    }

    @Test
    @SneakyThrows
    void getRequestResponseMustSuccessReturnText() {
        String requestText = "test";
        mockServer.stubFor(post("/")
                .withRequestBody(matchingJsonPath("$.data", equalTo(requestText)))
                .willReturn(aResponse()
                        .withStatus(HttpStatus.OK.value())));
//        UUID requestId = UUID.fromString("68fd8e0d-f1ba-4eae-8ba9-172deee2cc43");
//        sendToTopic(requestId, requestText.toUpperCase());
        mockServer.addMockServiceRequestListener((request, response) -> {
            try {
                TextClient.Request value = mapper.readValue(request.getBody(), TextClient.Request.class);
                sendToTopic(value.getRequestId(), value.getData().toUpperCase());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        Request testRequest = new Request();
        testRequest.setText(requestText);

        String res = testClient.post()
                .uri("/request-response")
                .body(BodyInserters.fromValue(testRequest))
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("TEST", res);
    }

    @DynamicPropertySource
    public static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("mockServerURL", mockServer::baseUrl);
    }

    private Map<String, Object> kafkaProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddresses);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-sender");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        return props;
    }

    private void sendToTopic(UUID rqId, String value) {
        ProducerRecord<Integer, String> record =  new ProducerRecord<>("topic-for-demo",
                value);
        record.headers()
                .add("RQ_ID", rqId.toString().getBytes());
        SenderRecord<Integer, String, UUID> senderRecord = SenderRecord.create(record, UUID.randomUUID());

        kafkaSender.send(Mono.just(senderRecord))
                .log()
                .subscribe();
    }
}
