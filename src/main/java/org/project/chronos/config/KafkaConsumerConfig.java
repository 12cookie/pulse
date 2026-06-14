package org.project.chronos.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;
import static org.project.chronos.constants.ChronosConstants.ALL_PACKAGES;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Autowired
    private EnvProperty envProperty;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Bean
    public ConsumerFactory<String, ChronosTaskMessage> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class.getName());
        props.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, ChronosTaskMessage.class.getName());
        props.put(JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS, Boolean.FALSE);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, ALL_PACKAGES);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, envProperty.getConsumerBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, envProperty.getKafkaGroupId());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, envProperty.getKafkaMaxPollInterval());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, envProperty.getKafkaMaxPollRecords());
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, Boolean.TRUE);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED.name().toLowerCase());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ChronosTaskMessage> kafkaListenerContainerFactory(
            @Autowired ConsumerFactory<String, ChronosTaskMessage> consumerFactory
    ) {
        consumerFactory.addListener(new MicrometerConsumerListener<>(meterRegistry));
        ConcurrentKafkaListenerContainerFactory<String, ChronosTaskMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(envProperty.getKafkaListenerConcurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setAfterRollbackProcessor(afterRollbackProcessor());
        factory.setCommonErrorHandler(retryErrorHandler());
        factory.getContainerProperties().setMicrometerEnabled(true);
        Map<String, String> micrometerTags = new HashMap<>();
        factory.getContainerProperties().setMicrometerTags(micrometerTags);
        return factory;
    }

    public DefaultAfterRollbackProcessor<String, ChronosTaskMessage> afterRollbackProcessor() {
        return new DefaultAfterRollbackProcessor<>(new FixedBackOff(0L, 0L));
    }

    public DefaultErrorHandler retryErrorHandler() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        return new DefaultErrorHandler(recoverer, new ExponentialBackOff(1000, 3));
    }
}
