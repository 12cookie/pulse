package org.project.chronos.raft;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.project.chronos.config.EnvProperty;
import org.project.chronos.kafka.ChronosProducer;
import org.project.chronos.model.AssignedTask;
import org.project.chronos.model.ChronosResultMessage;

import java.util.HashMap;
import java.util.Map;

import static org.project.chronos.constants.ChronosConstants.DELIMITER;
import static org.project.chronos.constants.ChronosConstants.retryAfterHeaderKey;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractTaskFlowHandler {

    private final EnvProperty envProperty;

    private final ChronosProducer<Object> chronosProducer;

    public void publishFailedTasks(AssignedTask assignedTask, String errorMessage) {
        int resubmissionCount = assignedTask.getTask().getResubmissionCount();
        assignedTask.getTask().getErrorMessage().put(
                resubmissionCount,
                assignedTask.getTaskExecutorClientId().concat(DELIMITER).concat(errorMessage));
        if (resubmissionCount >= envProperty.getChronosProcessRetries()) {
            publishCompletedTask(ChronosResultMessage.builder()
                    .taskId(assignedTask.getTask().getTaskId())
                    .success(false)
                    .errorMessage(assignedTask.getTask().getErrorMessage())
                    .build());
            return;
        }

        assignedTask.getTask().setResubmissionCount(resubmissionCount + 1);
        chronosProducer.publishKafkaEvent(
                assignedTask.getTask().getTaskId(),
                assignedTask.getTask(),
                envProperty.getChronosProcessRetryTopicPrefix().concat(String.valueOf(resubmissionCount)),
                new HashMap<>(Map.of(
                        retryAfterHeaderKey,
                        Long.toString(System.currentTimeMillis() + envProperty.getChronosProcessRetryIntervalMs()))));
        log.info("Published failed task successfully for task id: {}", assignedTask.getTask().getTaskId());
    }

    public void publishCompletedTask(ChronosResultMessage resultMessage) {
        chronosProducer.publishKafkaEvent(
                resultMessage.getTaskId(), resultMessage,
                envProperty.getChronosProcessCompletionTopic(), null);
        log.info("Published completed task for task id: {}", resultMessage.getTaskId());
    }
}
