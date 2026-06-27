package org.project.chronos.service;

import grpc.chronos.executor.ChronosTask;
import grpc.chronos.executor.GetTaskRequest;
import grpc.chronos.executor.ResultAcknowledgment;
import grpc.chronos.executor.ResultSubmissionRequest;
import org.project.chronos.model.ChronosTaskMessage;

import java.io.IOException;

public interface ChronosTaskManager {

    void addTaskToQueue(ChronosTaskMessage requestMessage) throws IOException;

    ChronosTask getChronosTask(GetTaskRequest taskRequest);

    ResultAcknowledgment submitTaskResult(ResultSubmissionRequest chronosTaskResult);

    int getPendingQueueSize() throws IOException;

}
