package org.project.chronos.raft.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.server.RaftServer;
import org.project.chronos.config.EnvProperty;
import org.project.chronos.model.AssignedTaskWrapper;
import org.project.chronos.model.ChronosTaskMessage;
import org.project.chronos.raft.TaskFlowHandler;
import org.project.chronos.raft.client.RaftJobClient;
import org.project.chronos.raft.server.RaftJobServer;
import org.project.chronos.util.CommonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import static org.project.chronos.constants.ChronosConstants.*;

@Slf4j
@Service
@ConditionalOnProperty(name = "smart.qc.enable.raft", havingValue = "true")
public class TaskFlowHandlerRaft implements TaskFlowHandler {

    @Autowired
    private RaftGroup raftGroup;

    @Autowired
    private RaftServer raftServer;

    @Autowired
    private EnvProperty envProperty;

    @Autowired
    private RaftJobClient raftClient;

    @Override
    public void addTaskToQueue(ChronosTaskMessage chronosTaskMessage) throws IOException {
        if (leaderNotElected()) {
            throw new IOException("Raft leader not elected yet; cannot enqueue job for refId: "
                    + chronosTaskMessage.getTaskId());
        }

        String jobListString = CommonUtil.mapObjectToString(chronosTaskMessage);
        RaftClientReply reply = raftClient.sendCommand(ADD_TASK_TO_QUEUE + COLON + jobListString);
        log.debug("Reply for command {}: {}", ADD_TASK_TO_QUEUE, reply.getMessage().getContent().toStringUtf8());
    }

    @Override
    public Optional<ChronosTaskMessage> getTask(String taskExecutorClientId) {
        if (leaderNotElected()) {
            return Optional.empty();
        }

        try {
            RaftClientReply reply = raftClient.sendCommand(GET_PENDING_TASK.concat(COLON).concat(taskExecutorClientId));
            String commandResponse = reply.getMessage().getContent().toStringUtf8();
            log.debug("Reply for command {}: {}", GET_PENDING_TASK, commandResponse);

            if (Objects.equals(commandResponse, NO_PENDING_JOB)) return Optional.empty();
            ChronosTaskMessage chronosTaskMessage = CommonUtil.mapStringToObject(
                    commandResponse,
                    ChronosTaskMessage.class);
            return Optional.of(chronosTaskMessage);

        } catch (IOException ex) {
            // TODO: Handle exception properly
            log.error("Error while getting job through state machine: {}", ex.getLocalizedMessage());
            return Optional.empty();
        }
    }

    @Override
    public Optional<AssignedTaskWrapper> getAssignedTask(String taskId) {
        String command = GET_ASSIGNED_TASK_FROM_MAP.concat(COLON).concat(taskId);
        try {
            RaftClientReply reply = raftClient.sendCommand(command);
            String commandResponse = reply.getMessage().getContent().toStringUtf8();
            log.debug("Reply for command {}: {}", GET_ASSIGNED_TASK_FROM_MAP, commandResponse);
            if (Objects.equals(commandResponse, NO_TASK_FOUND_IN_MAP)) {
                return Optional.empty();
            }

            return Optional.of(CommonUtil.mapStringToObject(commandResponse, AssignedTaskWrapper.class));
        } catch (IOException ex) {
            // TODO: Handle exception
            log.error("Error while getting job by refId through state machine: {}", ex.getLocalizedMessage());
            return Optional.empty();
        }
    }

    /**
     * Get the current size of the pending job queue from the Raft state machine.
     *
     * @return the number of jobs in the pending queue
     * @throws IOException if the query fails
     */
    @Override
    public int getQueueSize() throws IOException {
        try {
            String queryReply = raftClient.sendQuery(GET_PENDING_TASK_QUEUE_SIZE).getMessage().getContent().toStringUtf8();
            if (INVALID_QUERY.equals(queryReply)) {
                log.warn("Received INVALID_QUERY response from Raft state machine");
                throw new IOException("Received INVALID_QUERY response from Raft state machine");
            }

            log.debug("Pending queue size: {}", Integer.parseInt(queryReply));
            return Integer.parseInt(queryReply);
        } catch (IOException e) {
            // TODO: HANDLE EXCEPTION
            log.error("Failed to get pending queue size: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public boolean isLeaderElected() {
        if (raftServer == null) return false;
        try {
            return raftServer.getDivision(RaftJobServer.RAFT_GROUP_ID)
                    .getInfo()
                    .getLeaderId() != null;
        } catch (Exception e) {
            log.warn("Could not determine leader election status: {}", e.getMessage());
            log.debug("Stack trace: ", e);
            return false;
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
            log.debug("Handle expired tasks reply: {}", reply.getMessage().getContent().toStringUtf8());
        } catch (IOException e) {
            log.warn("Error while handling expired task: {}", e.getMessage());
            log.debug("Stack trace: ", e);
        }
    }

    private boolean leaderNotElected() {
        return !isLeaderElected();
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
