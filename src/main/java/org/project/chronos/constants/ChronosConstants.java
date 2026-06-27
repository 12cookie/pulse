package org.project.chronos.constants;

public interface ChronosConstants {

    String COLON = ":";

    String DELIMITER = "::";

    String HYPHEN = "-";

    String COMMA = ",";

    String ACK_ALL = "all";

    String PEER_PREFIX = "n";

    String ALL_PACKAGES = "*";

    String HOSTNAME = "HOSTNAME";

    String DEFAULT_POD = "pod-0";

    String retryAfterHeaderKey = "retryAfter";

    String LISTENER_ID = "chronosConsumerListener";

    String RETRY_LISTENER_ID = "chronosRetryConsumerListener";

    String RAFT_GROUP = "chronos-raft-group";

    String ADD_TASK_TO_QUEUE = "ADD_TASK_TO_QUEUE";

    String INVALID_ACTION = "INVALID_ACTION";

    String GET_PENDING_TASK = "GET_JOB_FROM_QUEUE";

    String GET_ASSIGNED_TASK_FROM_MAP = "GET_JOB_FROM_MAP";

    String REMOVE_TASK_FROM_MAP = "REMOVE_TASK_FROM_MAP";

    String HANDLE_EXPIRED_TASKS = "HANDLE_EXPIRED_JOBS";

    String JOB_ADDED_SUCCESSFULLY = "JOB_ADDED_SUCCESSFULLY";

    String TASK_REMOVED_SUCCESSFULLY = "TASK_REMOVED_SUCCESSFULLY";

    String NO_TASK_FOUND_IN_MAP = "NO_JOB_FOUND_IN_MAP";

    String NO_PENDING_JOB = "NO_PENDING_JOB";

    String QUERY_FORMATTER = "query:%s";

    String COMMAND_FORMATTER = "action:%s";

    String INVALID_QUERY = "INVALID_QUERY";

    String GET_PENDING_TASK_QUEUE_SIZE = "GET_PENDING_JOB_QUEUE_SIZE";

    String TASK_RESULT_ACK_FORMATTER = "Task result received for taskId: %s";

}
