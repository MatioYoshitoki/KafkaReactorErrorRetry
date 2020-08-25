package com.test.kafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.test.kafka.pojo.TestKafka;
import com.test.kafka.utils.TopicUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.util.UUID;


@RestController
@RequestMapping("/event")
public class CollectController {


    private final Logger log = LoggerFactory.getLogger(getClass());
    private final KafkaSender<String, String> sender;
    private final ObjectMapper objectMapper;

    public CollectController(
            @Qualifier("sender")
                    KafkaSender<String, String> sender,
            ObjectMapper objectMapper) {
        this.sender = sender;
        this.objectMapper = objectMapper;
    }

    @PostMapping("/collect")
    public Flux<Boolean> collect(@RequestBody TestKafka event) {

        return sender.send(Mono.just(buildSenderRecord(event, TopicUtils.NORMAL_TOPIC)))
                .map((SenderResult<String> result) -> true);
    }

    @PostMapping("/collect2")
    public Flux<Boolean> collect2(@RequestBody TestKafka event) {
        return sender.send(Mono.just(buildSenderRecord(event, "test_topic2")))
                .map((SenderResult<String> result) -> true);
    }

    private SenderRecord<String, String, String> buildSenderRecord(TestKafka event, String topic) {
        String message;
        try {
            message = objectMapper.writeValueAsString(event);
            log.debug("Event Message: " + message);
//            String topic = "test_topic";
            log.debug("Topic: " + topic);
            String uuid = UUID.randomUUID().toString();
            return SenderRecord.create(topic,
                    null,
                    System.currentTimeMillis(),
                    uuid, // 如果以 type 作为 key，会导致分区倾斜
                    message,
                    uuid);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }
}
