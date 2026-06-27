package org.project.chronos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.NonNull;
import org.project.chronos.config.EnvProperty;
import org.project.chronos.model.ChronosTaskMessage;
import org.project.chronos.service.ChronosTaskManager;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

import static org.project.chronos.constants.ChronosConstants.RETRY_LISTENER_ID;
import static org.project.chronos.constants.ChronosConstants.retryAfterHeaderKey;

@Slf4j
@Component
public class ChronosRetryListener implements ConsumerSeekAware {

    private final ChronosTaskManager chronosTaskManager;

    private final ScheduledExecutorService scheduledExecutorService;

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private final Map<TopicPartition, ScheduledFuture<?>> pendingResumes = new ConcurrentHashMap<>();

    private final ThreadLocal<ConsumerSeekAware.ConsumerSeekCallback> seekCallback = new ThreadLocal<>();

    public ChronosRetryListener(EnvProperty envProperty,
                                ChronosTaskManager chronosTaskManager,
                                KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        this.chronosTaskManager = chronosTaskManager;
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(
                envProperty.getNumberOfPartition(),
                Thread.ofVirtual().factory());
    }

    @Override
    public void registerSeekCallback(@NonNull ConsumerSeekCallback callback) {
        seekCallback.set(callback);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        seekCallback.remove();
    }

    @KafkaListener(
            id = RETRY_LISTENER_ID,
            topicPattern = "${chronos.process.retry.topic:CHRONOS.PROCESS.RETRY.TOPIC-.*}")
    public void onRetryMessage(ConsumerRecord<String, ChronosTaskMessage> consumerRecord,
                               Acknowledgment acknowledgment) {
        long retryAfter = Long.parseLong(new String(
                consumerRecord.headers().lastHeader(retryAfterHeaderKey).value(),
                StandardCharsets.UTF_8));
        long currentTime = System.currentTimeMillis();
        if (currentTime < retryAfter) {
            long delay = retryAfter - currentTime;
            log.info("Retry after {} milliseconds", delay);
            TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
            seekCallback.get().seek(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
            acknowledgment.acknowledge();
            MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(RETRY_LISTENER_ID);
            if (!Objects.isNull(container)) {
                container.pausePartition(topicPartition);
                pendingResumes.compute(topicPartition, (partition, existing) -> {
                    if (!Objects.isNull(existing) && !existing.isDone()) {
                        existing.cancel(false);
                    }

                    return scheduledExecutorService.schedule(() -> {
                        pendingResumes.remove(partition);
                        container.resumePartition(partition);
                    }, delay, TimeUnit.MILLISECONDS);
                });
                return;
            }
        }

        log.info("Received retry message - key: {}, topic: {}", consumerRecord.key(), consumerRecord.topic());
        try {
            MDC.put("refId", consumerRecord.key());
            chronosTaskManager.addTaskToQueue(consumerRecord.value());
            log.info("Message consumed - key: {}, topic: {}", consumerRecord.key(), consumerRecord.topic());
        } catch (IOException e) {
            log.error("Error occurred while consuming message", e);
            throw new RuntimeException(e);
        } finally {
            MDC.clear();
            acknowledgment.acknowledge();
        }
    }
}
