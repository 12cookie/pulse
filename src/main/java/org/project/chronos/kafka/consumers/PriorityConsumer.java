package org.project.chronos.kafka.consumers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.project.chronos.model.ChronosTaskMessage;
import org.project.chronos.service.ChronosTaskManager;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static org.project.chronos.constants.ChronosConstants.PRIORITY_LISTENER_ID;

@Slf4j
@Component
public class PriorityConsumer {

    @Autowired
    private ChronosTaskManager chronosTaskManager;

    @KafkaListener(
            id = PRIORITY_LISTENER_ID,
            topics = {"${chronos.process.priority.topic:CHRONOS.PROCESS.PIORITY.TOPIC}"})
    public void onMessage(ConsumerRecord<String, ChronosTaskMessage> consumerRecord, Acknowledgment acknowledgment) {
        log.info("Received message - key: {}, topic: {}", consumerRecord.key(), consumerRecord.topic());
        try {
            MDC.put("refId", consumerRecord.key());
            chronosTaskManager.addPriorityTaskToQueue(consumerRecord.value());
            log.info("Priority message consumed - key: {}, topic: {}", consumerRecord.key(), consumerRecord.topic());
        } catch (IOException e) {
            log.error("Error occurred while consuming message", e);
            throw new RuntimeException(e);
        } finally {
            MDC.clear();
            acknowledgment.acknowledge();
        }
    }
}
