package com.test.kafka.collector;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.TransactionManager;

public interface StreamingCollector {


    Flux<ReceiverRecord<String, String>> receive();

    Flux<ReceiverRecord<String, String>> receiveExactlyOnce(TransactionManager transactionManager);

}
