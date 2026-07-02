package org.project.chronos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
@Component
public class ChronosProducer<T> {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void publishKafkaEvent(String key, T eventMessage, String topic) {
        ProducerRecord<String, Object> producerRecord = buildProducerRecord(key, eventMessage, topic);
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(producerRecord);
        try {
            SendResult<String, Object> result = future.get();
            log.info("Message Sent SuccessFully for the key : {} and the topic is {} , partition is {}",
                    key, topic, result.getProducerRecord().partition());
        } catch (InterruptedException | RuntimeException | ExecutionException e) {
            log.error("Error while sending the message to topic {} for the key {} with error {}",
                    topic, key, e.getMessage());
            log.debug("Exception details: ", e);
        }
    }

    private ProducerRecord<String, Object> buildProducerRecord(String key, Object value, String topic) {
        log.debug("Building Producer Record");
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

}
