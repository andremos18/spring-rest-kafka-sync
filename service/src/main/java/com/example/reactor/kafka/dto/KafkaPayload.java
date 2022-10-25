package com.example.reactor.kafka.dto;

import lombok.Data;

import java.util.UUID;

@Data
public class KafkaPayload {
    private UUID rqId;
    private String value;
}
