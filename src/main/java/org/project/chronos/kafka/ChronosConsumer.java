package org.project.chronos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.project.chronos.config.EnvProperty;
import org.project.chronos.model.ChronosTaskMessage;
import org.project.chronos.service.ChronosTaskManager;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.project.chronos.constants.ChronosConstants.LISTENER_ID;

@Slf4j
@Component
@EnableScheduling
public class ChronosConsumer {

    @Autowired
    private EnvProperty envProperty;

    @Autowired
    private ChronosTaskManager chronosTaskManager;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private final AtomicBoolean isPaused = new AtomicBoolean(false);

    /**
     * Pause the Kafka consumer immediately after startup so no messages
     * are processed until a Raft leader is elected.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void pauseOnStartup() {
        MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(LISTENER_ID);
        if (container != null) {
            container.pause();
            isPaused.set(true);
            log.info("Kafka consumer paused on startup — waiting for Raft leader election");
        }
    }

    @KafkaListener(
            id = LISTENER_ID,
            topics = {"${chronos.process.initiation.topic:CHRONOS.PROCESS.INITIATION.TOPIC}"})
    public void onMessage(ConsumerRecord<String, ChronosTaskMessage> consumerRecord, Acknowledgment acknowledgment) {
        log.info("Received message - key: {}, topic: {}", consumerRecord.key(), consumerRecord.topic());
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

    /**
     * Periodically checks Raft leader status and queue size to pause/resume
     * the Kafka consumer accordingly.
     * - If no leader is elected yet: keep paused.
     * - If leader is elected: apply queue-size thresholds as normal.
     */
    @Scheduled(
            initialDelayString = "${kafka.consumer.monitor.initial.delay.ms:10000}",
            fixedDelayString = "${kafka.consumer.monitor.interval.ms:5000}")
    public void monitorQueueAndControlConsumer() {
        try {
            MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(LISTENER_ID);
            if (listenerContainer == null) {
                log.warn("Kafka listener container '{}' not found", LISTENER_ID);
                return;
            }

            if (chronosTaskManager.isRaftStable()) {
                monitorTaskQueue(listenerContainer);
                return;
            }

            pauseKafkaConsumption(listenerContainer);
        } catch (Exception e) {
            log.error("Error monitoring queue: {}", e.getMessage());
            log.debug("Stack trace: ", e);
        }
    }

    private void pauseKafkaConsumption(MessageListenerContainer listenerContainer) {
        if (!isPaused.get()) {
            log.warn("Raft leader not elected — pausing Kafka consumer");
            listenerContainer.pause();
            isPaused.set(true);
        } else {
            log.debug("Waiting for Raft leader election...");
        }
    }

    private void monitorTaskQueue(MessageListenerContainer listenerContainer) throws IOException {
        int pendingQueueSize = chronosTaskManager.getPendingQueueSize();
        if (pendingQueueSize >= envProperty.getMaxRaftQueueSize() && !isPaused.get()) {
            log.warn("Queue size {} exceeded max limit {}, pausing Kafka consumer",
                    pendingQueueSize, envProperty.getMaxRaftQueueSize());
            listenerContainer.pause();
            isPaused.set(true);
            log.info("Kafka consumer paused successfully");
        } else if (pendingQueueSize <= envProperty.getMinRaftQueueSize() && isPaused.get()) {
            log.info("Queue size {} below min limit {}, resuming Kafka consumer",
                    pendingQueueSize, envProperty.getMinRaftQueueSize());
            listenerContainer.resume();
            isPaused.set(false);
            log.info("Kafka consumer resumed successfully");
        }
    }
}

