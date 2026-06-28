package org.project.chronos.raft;

import org.project.chronos.model.AssignedTask;
import org.project.chronos.model.AssignedTaskWrapper;
import org.project.chronos.model.ChronosResultMessage;
import org.project.chronos.model.ChronosTaskMessage;

import java.io.IOException;
import java.util.Optional;

public interface TaskFlowHandler {

    void addTaskToQueue(ChronosTaskMessage chronosTaskMessage) throws IOException;

    void addPriorityTaskToQueue(ChronosTaskMessage chronosTaskMessage) throws IOException;

    Optional<ChronosTaskMessage> getTask(String taskExecutorClientId);

    Optional<AssignedTaskWrapper> getAssignedTask(String taskId);

    void removeTaskFromMap(long key);

    int getQueueSize() throws IOException;

    int getPriorityQueueSize() throws IOException;

    void publishFailedTasks(AssignedTask assignedTask, String errorMessage);

    void publishCompletedTask(ChronosResultMessage resultMessage);
}
