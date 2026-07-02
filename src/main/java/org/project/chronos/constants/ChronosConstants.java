package org.project.chronos.constants;

public interface ChronosConstants {
    String COLON = ":";

    String DELIMITER = "::";

    String ACK_ALL = "all";

    String PEER_PREFIX = "n";

    String ALL_PACKAGES = "*";

    String HOSTNAME = "HOSTNAME";

    String DEFAULT_POD = "pod-0";

    String LISTENER_ID = "chronosListener";

    String RETRY_LISTENER_ID = "chronosRetryListener";

    String PRIORITY_RETRY_LISTENER_ID = "chronosPriorityRetryListener";

    String PRIORITY_LISTENER_ID = "chronosPriorityListener";

    String RAFT_GROUP = "chronos-raft-group";

    String ADD_TASK_TO_QUEUE = "ADD_TASK_TO_QUEUE";

    String ADD_PRIORITY_TASK_TO_QUEUE = "ADD_PRIORITY_TASK_TO_QUEUE";

    String GET_PENDING_TASK = "GET_JOB_FROM_QUEUE";

    String GET_ASSIGNED_TASK_FROM_MAP = "GET_JOB_FROM_MAP";

    String REMOVE_TASK_FROM_MAP = "REMOVE_TASK_FROM_MAP";

    String HANDLE_EXPIRED_TASKS = "HANDLE_EXPIRED_JOBS";

    String TASK_ADDED_SUCCESSFULLY = "TASK_ADDED_SUCCESSFULLY";

    String TASK_REMOVED_SUCCESSFULLY = "TASK_REMOVED_SUCCESSFULLY";

    String NO_TASK_FOUND_IN_MAP = "NO_JOB_FOUND_IN_MAP";

    String NO_PENDING_JOB = "NO_PENDING_JOB";

    String GET_PENDING_TASK_QUEUE_SIZE = "GET_PENDING_JOB_QUEUE_SIZE";

    String GET_PRIORITY_TASK_QUEUE_SIZE = "GET_PRIORITY_TASK_QUEUE_SIZE";

    String TASK_RESULT_ACK_FORMATTER = "Task result received for taskId: %s";
}
