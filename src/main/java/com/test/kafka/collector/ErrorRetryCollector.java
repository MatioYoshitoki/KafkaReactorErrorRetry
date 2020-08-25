package com.test.kafka.collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.TransactionManager;

import java.time.Duration;

@Component("errorRetryCollector")
public class ErrorRetryCollector implements StreamingCollector {

    private final Logger log = LoggerFactory.getLogger(ErrorRetryCollector.class);
    private final KafkaReceiver<String, String> receiver;


    public ErrorRetryCollector(
            @Qualifier("errorRetryReceiver")
                    KafkaReceiver<String, String> receiver
    ) {
        this.receiver = receiver;
    }

    @Override
    public Flux<ReceiverRecord<String, String>> receive() {
        log.info("Start receive Message from kafka");
        return receiver.receive().delaySequence(Duration.ofSeconds(10));
    }

    @Override
    public Flux<ReceiverRecord<String, String>> receiveExactlyOnce(TransactionManager transactionManager) {
        return null;
    }
}
