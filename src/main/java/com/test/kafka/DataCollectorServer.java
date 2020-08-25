package com.test.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication(scanBasePackages = "com.test.kafka")
@EnableKafka
public class DataCollectorServer implements CommandLineRunner {

    private final ErrorRetryCollectorExecutor errorRetryCollectorExecutor;
    private final NormalCollectorExecutor normalCollectorExecutor;

    private final Logger log = LoggerFactory.getLogger(getClass());

    public DataCollectorServer(ErrorRetryCollectorExecutor errorRetryCollectorExecutor, NormalCollectorExecutor normalCollectorExecutor) {
        this.errorRetryCollectorExecutor = errorRetryCollectorExecutor;
        this.normalCollectorExecutor = normalCollectorExecutor;
    }

    public static void main(String[] args) {
        SpringApplication.run(DataCollectorServer.class);
    }

    @Override
    public void run(String... args) {
        log.info("StreamingCollector Started");
        errorRetryCollectorExecutor.execute();
        normalCollectorExecutor.execute();
    }
}
