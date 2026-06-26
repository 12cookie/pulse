package org.project.chronos.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

import static org.project.chronos.constants.ChronosConstants.ACK_ALL;

@Configuration
public class KafkaProducerConfig {

    @Autowired
    private EnvProperty envProperty;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, envProperty.getProducerBootstrapServers());
        configProps.put(ProducerConfig.ACKS_CONFIG, ACK_ALL);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, envProperty.getProducerLingerMs());
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, envProperty.getProducerBatchSize());
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, envProperty.getProducerMaxInFlightRequests());
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, envProperty.getProducerDeliveryTimeoutMs());
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, envProperty.getProducerRequestTimeoutMs());
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, envProperty.getProducerRetryBackoffMs());
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
