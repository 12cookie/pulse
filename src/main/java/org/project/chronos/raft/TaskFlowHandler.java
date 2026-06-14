package org.project.chronos.raft;

import org.project.chronos.model.AssignedTaskWrapper;
import org.project.chronos.model.ChronosTaskMessage;

import java.io.IOException;
import java.util.Optional;

public interface TaskFlowHandler {

    void addTaskToQueue(ChronosTaskMessage chronosTaskMessage) throws IOException;

    Optional<ChronosTaskMessage> getTask(String taskExecutorClientId);

    Optional<AssignedTaskWrapper> getAssignedTask(String taskId);

    int getQueueSize() throws IOException;

    boolean isLeaderElected();
}
