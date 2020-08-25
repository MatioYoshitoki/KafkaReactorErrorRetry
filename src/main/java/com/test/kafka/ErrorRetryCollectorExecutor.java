package com.test.kafka;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.test.kafka.collector.StreamingCollector;
import com.test.kafka.pojo.TestKafka;
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

import java.util.Date;
import java.util.UUID;

@Component
public class ErrorRetryCollectorExecutor implements ApplicationEventPublisherAware {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ObjectMapper objectMapper;
    private final StreamingCollector streamingCollector;
    private final KafkaSender<String, String> sender;

    Scheduler streamingCollectorStoreScheduler = Schedulers.newElastic("streaming-collector-store");

    public ErrorRetryCollectorExecutor(
            @Qualifier("errorRetryCollector")
                    StreamingCollector streamingCollector,
            ObjectMapper objectMapper,
            @Qualifier("sender") KafkaSender<String, String> sender) {

        this.streamingCollector = streamingCollector;
        this.objectMapper = objectMapper;
        this.sender=sender;
    }

    public void executeV2() {
        streamingCollector.receive()
                .onErrorContinue((err, it) -> log.error(err.getMessage()))
                .publishOn(streamingCollectorStoreScheduler)
                .doOnNext(it -> {
                    long createTime = it.timestamp();
                    TestKafka testKafka = null;
                    try {
                        testKafka = objectMapper.readValue(it.value(), TestKafka.class);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    assert testKafka != null;
                    long diff = (System.currentTimeMillis() - testKafka.getProcessTime()*1000)/1000;
                    if (diff>100) {
                        log.info("完全失败，发往error topic");
                        sender.send(Mono.just(buildSenderRecord(testKafka, TopicUtils.ERROR_TOPIC))).subscribe();
                    }else {
                        if((diff>=30 && diff<40) || (diff>=60 && diff<70) || (diff>=90 && diff<100)) {
                            log.info("符合重试要求，发回normal topic");
                            sender.send(Mono.just(buildSenderRecord(testKafka, TopicUtils.NORMAL_TOPIC))).subscribe();
                        } else {
                            log.info("未能符合要求，留在error retry topic, offset:"+it.offset());
                            sender.send(Mono.just(buildSenderRecord(testKafka, TopicUtils.ERROR_RETRY_TOPIC))).subscribe();
                        }
                    }
                    log.debug("ACK " + it.receiverOffset().offset());
                    it.receiverOffset().acknowledge();
                    it.receiverOffset().commit();
                })
                .subscribe(
                        (record) -> log.debug(""),
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
