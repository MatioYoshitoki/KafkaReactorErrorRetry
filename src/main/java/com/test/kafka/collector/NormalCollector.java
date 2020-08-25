package com.test.kafka.collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.TransactionManager;

@Component("normalCollector")
public class NormalCollector implements StreamingCollector {

    private final Logger log = LoggerFactory.getLogger(NormalCollector.class);
    private final KafkaReceiver<String, String> receiver;


    public NormalCollector(
            @Qualifier("normalReceiver")
                    KafkaReceiver<String, String> receiver
    ) {
        this.receiver = receiver;
    }

    @Override
    public Flux<ReceiverRecord<String, String>> receive() {
        log.info("Start receive Message from kafka");
        return receiver.receive();
    }

    @Override
    public Flux<ReceiverRecord<String, String>> receiveExactlyOnce(TransactionManager transactionManager) {
        return null;
    }
}
