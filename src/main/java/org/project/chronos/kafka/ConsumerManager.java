package org.project.chronos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.project.chronos.config.EnvProperty;
import org.project.chronos.service.ChronosTaskManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.project.chronos.constants.ChronosConstants.*;

@Slf4j
@Component
@EnableScheduling
public class ConsumerManager {

    @Autowired
    private EnvProperty envProperty;

    @Autowired
    private ChronosTaskManager chronosTaskManager;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private final AtomicBoolean isContainerPaused = new AtomicBoolean(false);

    private final AtomicBoolean isRetryContainerPaused = new AtomicBoolean(false);

    private final AtomicBoolean isPriorityContainerPaused = new AtomicBoolean(false);

    @EventListener(ApplicationReadyEvent.class)
    public void pauseOnStartup() {
        MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(LISTENER_ID);
        MessageListenerContainer retryContainer = kafkaListenerEndpointRegistry.getListenerContainer(RETRY_LISTENER_ID);
        MessageListenerContainer priorityContainer = kafkaListenerEndpointRegistry.getListenerContainer(PRIORITY_LISTENER_ID);
        pauseKafkaContainers(container, retryContainer, priorityContainer);
    }


    @Scheduled(
            initialDelayString = "${kafka.consumer.monitor.initial.delay.ms:10000}",
            fixedDelayString = "${kafka.consumer.monitor.interval.ms:5000}")
    public void monitorQueueAndControlConsumer() {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(LISTENER_ID);
        MessageListenerContainer retryListenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(RETRY_LISTENER_ID);
        MessageListenerContainer priorityContainer = kafkaListenerEndpointRegistry.getListenerContainer(PRIORITY_LISTENER_ID);
        try {
            if (listenerContainer == null || retryListenerContainer == null || priorityContainer == null) {
                log.warn("Kafka listeners not found");
                return;
            }

            monitorTaskQueue(listenerContainer, retryListenerContainer, priorityContainer);

        } catch (Exception e) {
            pauseKafkaContainers(listenerContainer, retryListenerContainer, priorityContainer);
            log.error("Error monitoring queue: {}", e.getMessage());
            log.debug("Stack trace: ", e);
        }
    }

    private void pauseKafkaContainers(MessageListenerContainer listenerContainer,
                                      MessageListenerContainer retryListenerContainer,
                                      MessageListenerContainer priorityContainer) {
        if (!isContainerPaused.get() || !isRetryContainerPaused.get() || !isPriorityContainerPaused.get()) {
            log.warn("Raft leader not elected — pausing Kafka listeners");
            listenerContainer.pause();
            retryListenerContainer.pause();
            priorityContainer.pause();
            isContainerPaused.set(true);
            isRetryContainerPaused.set(true);
            isPriorityContainerPaused.set(true);
        }
    }

    private void monitorTaskQueue(MessageListenerContainer listenerContainer,
                                  MessageListenerContainer retryListenerContainer,
                                  MessageListenerContainer priorityContainer) throws IOException {
        int pendingQueueSize = chronosTaskManager.getPendingQueueSize();
        int priorityQueueSize = chronosTaskManager.getPriorityQueueSize();
        if (pendingQueueSize >= envProperty.getMaxRaftQueueSize()
                && (!isContainerPaused.get() || !isRetryContainerPaused.get())) {
            log.warn("Queue size {} exceeded max limit {}, pausing Kafka listeners",
                    pendingQueueSize, envProperty.getMaxRaftQueueSize());
            listenerContainer.pause();
            retryListenerContainer.pause();
            isContainerPaused.set(true);
            isRetryContainerPaused.set(true);
            log.info("Kafka listeners paused successfully");
        } else if (pendingQueueSize <= envProperty.getMinRaftQueueSize()
                && (isContainerPaused.get() || isRetryContainerPaused.get())) {
            log.info("Queue size {} below min limit {}, resuming Kafka listeners",
                    pendingQueueSize, envProperty.getMinRaftQueueSize());
            listenerContainer.resume();
            retryListenerContainer.resume();
            isContainerPaused.set(false);
            isRetryContainerPaused.set(false);
            log.info("Kafka listeners resumed successfully");
        }

        if (priorityQueueSize >= envProperty.getMaxRaftQueueSize() && !isPriorityContainerPaused.get()) {
            log.warn("Priority queue size {} exceeded max limit {}, pausing Kafka listener",
                    priorityQueueSize, envProperty.getMaxRaftQueueSize());
            priorityContainer.pause();
            isPriorityContainerPaused.set(true);
        } else if (priorityQueueSize <= envProperty.getMinRaftQueueSize() && isPriorityContainerPaused.get()) {
            log.info("Priority queue size {} below min limit {}, resuming Kafka listener",
                    pendingQueueSize, envProperty.getMinRaftQueueSize());
            priorityContainer.resume();
            isPriorityContainerPaused.set(false);
            log.info("Kafka listener resumed successfully");
        }
    }
}
