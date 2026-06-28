package org.project.chronos.config;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jspecify.annotations.NonNull;
import org.project.chronos.model.ChronosTaskMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;
import static org.project.chronos.constants.ChronosConstants.ALL_PACKAGES;

@Slf4j
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Autowired
    private EnvProperty envProperty;

    @Autowired
    private MeterRegistry meterRegistry;

    @Bean
    public ConsumerFactory<String, ChronosTaskMessage> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, envProperty.getConsumerBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, envProperty.getKafkaGroupId());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, envProperty.getKafkaMaxPollInterval());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, envProperty.getKafkaMaxPollRecords());
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, Boolean.TRUE);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED.name().toLowerCase());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);
        JsonMapper chronosObjectMapper = JsonMapper.builder()
                .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
                .configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, true)
                .build();
        JacksonJsonDeserializer<ChronosTaskMessage> jsonDeserializer = new JacksonJsonDeserializer<>(
                ChronosTaskMessage.class,
                chronosObjectMapper);
        jsonDeserializer.setUseTypeHeaders(false);
        jsonDeserializer.addTrustedPackages(ALL_PACKAGES);
        ErrorHandlingDeserializer<ChronosTaskMessage> errorHandlingDeserializer = new ErrorHandlingDeserializer<>(jsonDeserializer);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), errorHandlingDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ChronosTaskMessage> kafkaListenerContainerFactory(
            ConsumerFactory<String, ChronosTaskMessage> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate) {
        consumerFactory.addListener(new MicrometerConsumerListener<>(meterRegistry));
        ConcurrentKafkaListenerContainerFactory<String, ChronosTaskMessage> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(envProperty.getKafkaListenerConcurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setMicrometerEnabled(true);
        DefaultErrorHandler errorHandler = getDefaultErrorHandler(kafkaTemplate);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    private @NonNull DefaultErrorHandler getDefaultErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        FixedBackOff noRetryBackOff = new FixedBackOff(0L, 0);
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> {
                    log.error("DeadLetter Recoverer Error for key: {}, message: {}", record.key(), exception.getMessage());
                    return new TopicPartition(envProperty.getChronosProcessCompletionTopic(), -1);
                });

        return new DefaultErrorHandler(recoverer, noRetryBackOff);
    }
}
