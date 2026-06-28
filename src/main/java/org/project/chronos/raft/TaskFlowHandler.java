package org.project.chronos.raft;

import org.project.chronos.model.AssignedTask;
import org.project.chronos.model.AssignedTaskWrapper;
import org.project.chronos.model.ChronosResultMessage;
import org.project.chronos.model.ChronosTaskMessage;

import java.io.IOException;
import java.util.Optional;

/**
 * Defines the task flow operations exposed by the Raft-backed task handler.
 */
public interface TaskFlowHandler {

    /**
     * Add a task to the normal pending queue.
     *
     * @param chronosTaskMessage the task to enqueue
     * @throws IOException if the underlying Raft command fails
     */
    void addTaskToQueue(ChronosTaskMessage chronosTaskMessage) throws IOException;

    /**
     * Add a task to the priority queue.
     *
     * @param chronosTaskMessage the task to enqueue
     * @throws IOException if the underlying Raft command fails
     */
    void addPriorityTaskToQueue(ChronosTaskMessage chronosTaskMessage) throws IOException;

    /**
     * Fetch the next available task for a worker.
     *
     * @param taskExecutorClientId the worker identifier
     * @return the next task if one is available
     */
    Optional<ChronosTaskMessage> getTask(String taskExecutorClientId);

    /**
     * Look up an assigned task by task identifier.
     *
     * @param taskId the task identifier
     * @return the assigned task wrapper if present
     */
    Optional<AssignedTaskWrapper> getAssignedTask(String taskId);

    /**
     * Remove a task from the assigned-task map.
     *
     * @param key the assigned-task key
     */
    void removeTaskFromMap(long key);

    /**
     * Get the size of the pending queue.
     *
     * @return the number of pending tasks
     * @throws IOException if the underlying Raft query fails
     */
    int getQueueSize() throws IOException;

    /**
     * Get the size of the priority queue.
     *
     * @return the number of priority tasks
     * @throws IOException if the underlying Raft query fails
     */
    int getPriorityQueueSize() throws IOException;

    /**
     * Publish a failed task for retry or terminal failure handling.
     *
     * @param assignedTask the assigned task that failed
     * @param errorMessage the failure reason
     */
    void publishFailedTasks(AssignedTask assignedTask, String errorMessage);

    /**
     * Publish a completed task result.
     *
     * @param resultMessage the task result to publish
     */
    void publishCompletedTask(ChronosResultMessage resultMessage);
}
