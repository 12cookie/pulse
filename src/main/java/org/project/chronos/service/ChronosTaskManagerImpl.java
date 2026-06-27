package org.project.chronos.service;

import grpc.chronos.executor.ChronosTask;
import grpc.chronos.executor.GetTaskRequest;
import grpc.chronos.executor.ResultAcknowledgment;
import grpc.chronos.executor.ResultSubmissionRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.project.chronos.model.AssignedTask;
import org.project.chronos.model.AssignedTaskWrapper;
import org.project.chronos.model.ChronosResultMessage;
import org.project.chronos.model.ChronosTaskMessage;
import org.project.chronos.raft.TaskFlowHandler;
import org.project.chronos.util.CommonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Optional;

import static org.project.chronos.constants.ChronosConstants.TASK_RESULT_ACK_FORMATTER;

@Slf4j
@Service
public class ChronosTaskManagerImpl implements ChronosTaskManager {

    @Autowired
    private TaskFlowHandler taskFlowHandler;

    @Override
    public void addTaskToQueue(ChronosTaskMessage requestMessage) throws IOException {
        log.info("Received scraping request!!");
        taskFlowHandler.addTaskToQueue(requestMessage);
    }

    @Override
    public ChronosTask getChronosTask(GetTaskRequest getTaskRequest) {
        String taskExecutorClientId = getTaskRequest.getTaskExecutorClientId();
        if (StringUtils.isBlank(taskExecutorClientId)) {
            log.warn("Task executor client ID is blank in the request.");
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Task executor client ID is blank."));
        }

        Optional<ChronosTaskMessage> task = taskFlowHandler.getTask(taskExecutorClientId);
        return task.map(CommonUtil::createScrapingTaskRequest)
                .orElseGet(() -> ChronosTask.newBuilder().setTaskAvailable(false).build());
    }

    @Override
    public ResultAcknowledgment submitTaskResult(ResultSubmissionRequest chronosTaskResult) {
        Optional<AssignedTaskWrapper> task = taskFlowHandler.getAssignedTask(chronosTaskResult.getTaskId());
        if (task.isEmpty()) {
            return ResultAcknowledgment.newBuilder()
                    .setMessage("No task found for taskId: " + chronosTaskResult.getTaskId())
                    .build();
        }

        AssignedTask assignedTask = task.get().getAssignedTaskEntry().getValue();
        if (!chronosTaskResult.getSuccess()) {
            taskFlowHandler.publishFailedTasks(assignedTask, chronosTaskResult.getErrorMessage());
        } else {
            taskFlowHandler.publishCompletedTask(ChronosResultMessage.builder()
                    .taskId(chronosTaskResult.getTaskId())
                    .success(true)
                    .taskResult(chronosTaskResult.getResultData())
                    .taskExecutorId(task.get().getAssignedTaskEntry().getValue().getTaskExecutorClientId())
                    .errorMessage(assignedTask.getTask().getErrorMessage())
                    .build());
        }

        taskFlowHandler.removeTaskFromMap(task.get().getAssignedTaskEntry().getKey());
        return ResultAcknowledgment.newBuilder()
                .setMessage(String.format(TASK_RESULT_ACK_FORMATTER, assignedTask.getTask().getTaskId()))
                .build();
    }

    @Override
    public int getPendingQueueSize() throws IOException {
        return taskFlowHandler.getQueueSize();
    }
}
