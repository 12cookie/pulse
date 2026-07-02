package org.project.chronos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.project.chronos.config.EnvProperty;
import org.project.chronos.service.ChronosTaskManager;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.project.chronos.constants.ChronosConstants.*;

@Slf4j
@Component
@EnableScheduling
public class ConsumerManager {

    private final EnvProperty envProperty;

    private final ChronosTaskManager chronosTaskManager;

    private final Map<String, MessageListenerContainer> containerMap = new HashMap<>();

    private final AtomicBoolean isContainerPaused = new AtomicBoolean(false);

    private final AtomicBoolean isPriorityContainerPaused = new AtomicBoolean(false);

    public ConsumerManager(EnvProperty envProperty,
                           ChronosTaskManager chronosTaskManager,
                           KafkaListenerEndpointRegistry registry) {
        this.envProperty = envProperty;
        this.chronosTaskManager = chronosTaskManager;
        containerMap.put(LISTENER_ID, registry.getListenerContainer(LISTENER_ID));
        containerMap.put(RETRY_LISTENER_ID, registry.getListenerContainer(RETRY_LISTENER_ID));
        containerMap.put(PRIORITY_LISTENER_ID, registry.getListenerContainer(PRIORITY_LISTENER_ID));
        containerMap.put(PRIORITY_RETRY_LISTENER_ID, registry.getListenerContainer(PRIORITY_RETRY_LISTENER_ID));
    }

    @EventListener(ApplicationReadyEvent.class)
    public void pauseOnStartup() {
        pauseKafkaContainers();
    }

    @Scheduled(
            initialDelayString = "${kafka.consumer.monitor.initial.delay.ms:10000}",
            fixedDelayString = "${kafka.consumer.monitor.interval.ms:5000}")
    public void monitorListenerState() {
        try {
            int pendingQueueSize = chronosTaskManager.getPendingQueueSize();
            manageListenerState(containerMap.get(LISTENER_ID), containerMap.get(RETRY_LISTENER_ID), pendingQueueSize, isContainerPaused);
            int priorityQueueSize = chronosTaskManager.getPriorityQueueSize();
            manageListenerState(containerMap.get(PRIORITY_LISTENER_ID), containerMap.get(PRIORITY_RETRY_LISTENER_ID), priorityQueueSize, isPriorityContainerPaused);
        } catch (Exception e) {
            pauseKafkaContainers();
            log.error("Error monitoring queue: {}", e.getMessage());
            log.debug("Stack trace: ", e);
        }
    }

    private void pauseKafkaContainers() {
        if (!isContainerPaused.get() || !isPriorityContainerPaused.get()) {
            log.warn("Raft leader not elected — pausing Kafka listeners");
            containerMap.forEach((_, container) -> container.pause());
            isContainerPaused.set(true);
            isPriorityContainerPaused.set(true);
        }
    }

    private void manageListenerState(MessageListenerContainer container,
                                     MessageListenerContainer retryContainer,
                                     int pendingQueueSize, AtomicBoolean isContainerPaused) {
        if (pendingQueueSize >= envProperty.getMaxRaftQueueSize() && !isContainerPaused.get()) {
            log.warn("Queue size {} exceeded max limit {}, pausing Kafka listeners",
                    pendingQueueSize, envProperty.getMaxRaftQueueSize());
            container.pause();
            retryContainer.pause();
            isContainerPaused.set(true);
            log.info("Kafka listeners paused successfully");
        } else if (pendingQueueSize <= envProperty.getMinRaftQueueSize() && isContainerPaused.get()) {
            log.info("Queue size {} below min limit {}, resuming Kafka listeners",
                    pendingQueueSize, envProperty.getMinRaftQueueSize());
            container.resume();
            retryContainer.resume();
            isContainerPaused.set(false);
            log.info("Kafka listeners resumed successfully");
        }
    }
}
