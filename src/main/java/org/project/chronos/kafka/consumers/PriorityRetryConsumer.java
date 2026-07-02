package org.project.chronos.kafka.consumers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.project.chronos.model.ChronosTaskMessage;
import org.project.chronos.service.ChronosTaskManager;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static org.project.chronos.constants.ChronosConstants.PRIORITY_RETRY_LISTENER_ID;

@Slf4j
@Component
@RequiredArgsConstructor
public class PriorityRetryConsumer {

    private final ChronosTaskManager chronosTaskManager;

    @KafkaListener(
            id = PRIORITY_RETRY_LISTENER_ID,
            topicPattern = "${chronos.process.priority.retry.topic:CHRONOS.PROCESS.PRIORITY.RETRY.TOPIC-.*}")
    public void onRetryMessage(ConsumerRecord<String, ChronosTaskMessage> consumerRecord,
                               Acknowledgment acknowledgment) {

        log.info("Received priority retry message - key: {}, topic: {}", consumerRecord.key(), consumerRecord.topic());
        try {
            MDC.put("refId", consumerRecord.key());
            chronosTaskManager.addPriorityTaskToQueue(consumerRecord.value());
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
