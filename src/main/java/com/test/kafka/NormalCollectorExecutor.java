package com.test.kafka;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.test.kafka.collector.StreamingCollector;
import com.test.kafka.pojo.TestKafka;
import com.test.kafka.utils.MonoUtils;
import com.test.kafka.utils.TopicUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class NormalCollectorExecutor implements ApplicationEventPublisherAware {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ObjectMapper objectMapper;
    private final StreamingCollector streamingCollector;
    private final KafkaSender<String, String> sender;

    Scheduler streamingCollectorStoreScheduler = Schedulers.newElastic("streaming-collector-store");

    public NormalCollectorExecutor(
            @Qualifier("normalCollector")
                    StreamingCollector streamingCollector,
            ObjectMapper objectMapper,
            KafkaSender<String, String> sender) {
        this.streamingCollector = streamingCollector;
        this.objectMapper = objectMapper;
        this.sender = sender;
    }

    public void executeV2() {
        streamingCollector.receive()
                .onErrorContinue((err, it) -> log.error(err.getMessage()))
                .doOnNext(it -> {
                    log.debug("ACK " + it.receiverOffset().offset());
                    it.receiverOffset().acknowledge();
                })
                .flatMap(record -> MonoUtils.readJson(() -> {
                    try {
                        return objectMapper.readValue(record.value(), TestKafka.class);
                    } catch (JsonProcessingException e) {
                        log.error("JSON 解析出错, " + e.getMessage(), e);
                        return null;
                    }
                }).publishOn(streamingCollectorStoreScheduler)
                        .onErrorContinue((err, event) -> log.error("事件处理出错，触发错误处理事件 >>>>")))
                .onErrorContinue((err, event) -> log.error("事件处理出错，触发错误处理事件 >>>>"))
                .doOnNext(record-> {
//                    try {
//                        log.info("2号消费者消费："+objectMapper.writeValueAsString(record));
//                    } catch (JsonProcessingException e) {
//                        e.printStackTrace();
//                    }
                    if (record.getAge() >= 500) {
                        try {
                            log.info("保存成功："+ objectMapper.writeValueAsString(record));
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    }else {
                        try {
                            log.info("保存失败, 发往错误重试："+ objectMapper.writeValueAsString(record));
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        sender.send(Mono.just(buildSenderRecord(record, TopicUtils.ERROR_RETRY_TOPIC))).subscribe();
                    }
                })
                .subscribe(
                        (record) -> log.debug("name:" + record.getName() + ", age:" + record.getAge() + ", school:" + record.getSchool()),
                        (err) -> {
                            err.printStackTrace();
                            log.error("Kafka 事件处理出错，错误信息：" + err.getMessage(), err);
                        });

    }


    public void execute() {
        try {
            executeV2();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private SenderRecord<String, String, String> buildSenderRecord(TestKafka event, String topic) {
        String message;
        try {
            message = objectMapper.writeValueAsString(event);
            log.debug("Event Message: " + message);
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

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
    }

}
