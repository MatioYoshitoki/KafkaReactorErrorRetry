package com.test.kafka.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.org.apache.bcel.internal.generic.FADD;
import com.test.kafka.utils.TopicUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import javax.annotation.Resource;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
@Configuration
public class DataCollectorConfiguration {


    private final Logger log = LoggerFactory.getLogger(getClass());

    @Resource
    private ObjectMapper objectMapper;


    private final Integer serverPort = 8080;

    /**
     * reactor-kafka 接收器配置
     * <p>
     * update: 使用 reactor-kafka 重写数据接收，提高系统的稳定性
     */
    @Bean
    public ReceiverOptions<String, String> receiverOptions() {
        HashMap<String, Object> consumerProps = new HashMap<>();
        List<String> bootstrapServers = Collections.singletonList("127.0.0.1:9092");
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "123");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return ReceiverOptions.create(consumerProps);
    }

    @Bean
    public ReceiverOptions<String, String> receiverOptions2() {
        HashMap<String, Object> consumerProps = new HashMap<>();
        List<String> bootstrapServers = Collections.singletonList("127.0.0.1:9092");
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "124");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return ReceiverOptions.create(consumerProps);
    }

    @Bean
    public SenderOptions<String, String> senderOptions(KafkaProperties kafkaProperties) {
        HashMap<String, Object> producerProps = new HashMap<>();
        String ip = "127.0.0.1";
        List<String> bootstrapServers = kafkaProperties.getProducer().getBootstrapServers();
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = kafkaProperties.getBootstrapServers();
        }
//        String clientId = String.format("%s-%s-%s", "test_topic", ip, serverPort);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return SenderOptions.create(producerProps);
    }

    @Bean("errorRetryReceiver")
    public KafkaReceiver<String, String> receiver(ReceiverOptions<String, String> receiverOptions) {
        return KafkaReceiver.create(receiverOptions.subscription(Collections.singleton(TopicUtils.ERROR_RETRY_TOPIC))
//                        .commitInterval(Duration.ZERO)
//                        .commitBatchSize(0)
//                        .consumerProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
        );
    }

    @Bean("normalReceiver")
    public KafkaReceiver<String, String> receiver2(@Qualifier("receiverOptions2") ReceiverOptions<String, String> receiverOptions) {
        return KafkaReceiver.create(receiverOptions.subscription(Collections.singleton(TopicUtils.NORMAL_TOPIC)));
//                        .commitInterval(Duration.ofMillis(500))
//                        .consumerProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"));
    }


    @Bean("sender")
    public KafkaSender<String, String> sender(SenderOptions<String, String> senderOptions) {
        return KafkaSender.create(senderOptions);
    }

}
