package com.example.reactor.kafka.controllers;

import com.example.reactor.kafka.dto.Request;
import com.example.reactor.kafka.service.SampleService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SampleController {

    private final SampleService sampleService;

    @PostMapping("/request-response")
    public Mono<String> getRequestResponse(@RequestBody Request request) {
        return sampleService.get(request.getText());
    }
}
