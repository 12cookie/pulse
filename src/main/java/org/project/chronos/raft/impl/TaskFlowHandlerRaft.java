package org.project.chronos.raft.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.server.RaftServer;
import org.project.chronos.config.EnvProperty;
import org.project.chronos.kafka.ChronosProducer;
import org.project.chronos.model.AssignedTask;
import org.project.chronos.model.AssignedTaskWrapper;
import org.project.chronos.model.ChronosTaskMessage;
import org.project.chronos.raft.AbstractTaskFlowHandler;
import org.project.chronos.raft.TaskFlowHandler;
import org.project.chronos.raft.client.RaftTaskClient;
import org.project.chronos.util.CommonUtil;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import tools.jackson.core.type.TypeReference;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.project.chronos.constants.ChronosConstants.*;
import static org.project.chronos.util.CommonUtil.mapStringToObject;
import static org.project.chronos.util.CommonUtil.mapStringToTypeReference;

@Slf4j
@Service
@ConditionalOnProperty(name = "enable.raft", havingValue = "true")
public class TaskFlowHandlerRaft extends AbstractTaskFlowHandler implements TaskFlowHandler {

    private final RaftGroup raftGroup;

    private final RaftServer raftServer;

    private final EnvProperty envProperty;

    private final RaftTaskClient raftClient;

    public TaskFlowHandlerRaft(RaftGroup raftGroup, RaftServer raftServer, RaftTaskClient raftClient,
                               EnvProperty envProperty, ChronosProducer<Object> chronosProducer) {
        super(envProperty, chronosProducer);
        this.raftGroup = raftGroup;
        this.raftServer = raftServer;
        this.raftClient = raftClient;
        this.envProperty = envProperty;
    }

    @Override
    public void addTaskToQueue(ChronosTaskMessage chronosTaskMessage) throws IOException {
        String jobListString = CommonUtil.mapObjectToString(chronosTaskMessage);
        RaftClientReply reply = raftClient.sendCommand(ADD_TASK_TO_QUEUE.concat(COLON).concat(jobListString));
        log.info("Reply for command {}: {}", ADD_TASK_TO_QUEUE, reply.getMessage().getContent().toStringUtf8());
    }

    @Override
    public void addPriorityTaskToQueue(ChronosTaskMessage chronosTaskMessage) throws IOException {
        String jobListString = CommonUtil.mapObjectToString(chronosTaskMessage);
        RaftClientReply reply = raftClient.sendCommand(ADD_PRIORITY_TASK_TO_QUEUE.concat(COLON).concat(jobListString));
        log.info("Reply for command {}: {}", ADD_PRIORITY_TASK_TO_QUEUE, reply.getMessage().getContent().toStringUtf8());
    }

    @Override
    public Optional<ChronosTaskMessage> getTask(String taskExecutorClientId) {
        try {
            RaftClientReply reply = raftClient.sendCommand(GET_PENDING_TASK.concat(COLON).concat(taskExecutorClientId));
            String commandResponse = reply.getMessage().getContent().toStringUtf8();
            log.info("Reply for command {}: {}", GET_PENDING_TASK, commandResponse);

            if (Objects.equals(commandResponse, NO_PENDING_JOB)) return Optional.empty();
            ChronosTaskMessage chronosTaskMessage = mapStringToObject(
                    commandResponse,
                    ChronosTaskMessage.class);
            return Optional.of(chronosTaskMessage);

        } catch (IOException ex) {
            log.error("Error while getting job through state machine: {}", ex.getLocalizedMessage());
            return Optional.empty();
        }
    }

    @Override
    public Optional<AssignedTaskWrapper> getAssignedTask(String taskId) {
        String query = GET_ASSIGNED_TASK_FROM_MAP.concat(COLON).concat(taskId);
        try {
            RaftClientReply reply = raftClient.sendQuery(query);
            String commandResponse = reply.getMessage().getContent().toStringUtf8();
            log.info("Reply for query {}: {}", GET_ASSIGNED_TASK_FROM_MAP, commandResponse);
            if (Objects.equals(commandResponse, NO_TASK_FOUND_IN_MAP)) {
                return Optional.empty();
            }

            return Optional.of(mapStringToObject(commandResponse, AssignedTaskWrapper.class));
        } catch (IOException ex) {
            log.error("Error while getting job by refId through state machine: {}", ex.getLocalizedMessage());
            return Optional.empty();
        }
    }

    @Override
    public void removeTaskFromMap(long key) {
        String command = REMOVE_TASK_FROM_MAP.concat(COLON).concat(String.valueOf(key));
        try {
            RaftClientReply reply = raftClient.sendCommand(command);
            String commandResponse = reply.getMessage().getContent().toStringUtf8();
            log.info("Reply for command {}: {}", REMOVE_TASK_FROM_MAP, commandResponse);
        } catch (IOException ex) {
            log.warn("Error while getting job through state machine: {}", ex.getLocalizedMessage());
        }
    }

    @Override
    public int getQueueSize() throws IOException {
        try {
            String queryReply = raftClient.sendQuery(GET_PENDING_TASK_QUEUE_SIZE).getMessage().getContent().toStringUtf8();
            return Integer.parseInt(queryReply);
        } catch (IOException e) {
            log.error("Failed to get pending queue size: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public int getPriorityQueueSize() throws IOException {
        try {
            String queryReply = raftClient.sendQuery(GET_PRIORITY_TASK_QUEUE_SIZE).getMessage().getContent().toStringUtf8();
            return Integer.parseInt(queryReply);
        } catch (IOException e) {
            log.error("Failed to get pending queue size: {}", e.getMessage());
            throw e;
        }
    }

    @Scheduled(
            initialDelayString = "${expired.task.scheduler.initial.delay.ms:10000}",
            fixedRateString = "${expired.task.check.interval.ms:2000}")
    private void handleExpiredTasks() {
        if (!isCurrentNodeLeader()) {
            log.debug("Skipping expired task handler execution on follower");
            return;
        }

        log.info("Executing expired task handler on leader");
        try {
            String command = HANDLE_EXPIRED_TASKS
                    .concat(COLON)
                    .concat(String.valueOf(envProperty.getTaskTimeoutMs()));
            RaftClientReply reply = raftClient.sendCommand(command);
            List<AssignedTask> expiredTasks = mapStringToTypeReference(
                    reply.getMessage().getContent().toStringUtf8(),
                    new TypeReference<>() {});
            for (AssignedTask assignedTask : expiredTasks) {
                publishFailedTasks(assignedTask, "Task expired");
            }

            log.info("Total expired task: {}", expiredTasks.size());
        } catch (IOException e) {
            log.warn("Error while handling expired task: {}", e.getMessage());
            log.debug("Stack trace: ", e);
        }
    }

    private boolean isCurrentNodeLeader() {
        if (raftServer == null) return false;
        try {
            RaftServer.Division division = raftServer.getDivision(raftGroup.getGroupId());
            return division.getInfo().isLeader();
        } catch (IOException e) {
            log.warn("Could not determine leader status, exception: {}", e.getMessage());
            log.debug("Stack trace: ", e);
            return false;
        }
    }
}
