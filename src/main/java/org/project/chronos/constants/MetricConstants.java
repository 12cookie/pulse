package org.project.chronos.constants;

public interface MetricConstants {

    String timerPostFix = "_timer";

    String gaugePostFix = "_gauge";

    String counterPostfix = "_counter";

    /**
     * RAFT Metrics
     */
    String RAFT_QUERY_SUCCESS = "raft_query_success";
    String RAFT_QUERY_SUCCESS_DESC = "Counter to measure the number of successful query operations in RAFT";

    String RAFT_QUERY_FAILURE = "raft_query_failure";
    String RAFT_QUERY_FAILURE_DESC = "Counter to measure the number of failed query operations in RAFT";

    String RAFT_TRANSACTION_SUCCESS = "raft_transaction_success";
    String RAFT_TRANSACTION_SUCCESS_DESC = "Counter to measure the number of successful transaction operations in RAFT";

    String RAFT_TRANSACTION_FAILURE = "raft_transaction_failure";
    String RAFT_TRANSACTION_FAILURE_DESC = "Counter to measure the number of failed transaction operations in RAFT";

    String RAFT_TAKE_SNAPSHOT = "raft_take_snapshot";
    String RAFT_TAKE_SNAPSHOT_DESC = "Timer to measure the time taken to take a snapshot in Ratis";

    String RAFT_TAKE_SNAPSHOT_SUCCESS = "raft_take_snapshot_success";
    String RAFT_TAKE_SNAPSHOT_SUCCESS_DESC = "Counter to measure the number of successful take snapshot operations in Ratis";

    String RAFT_TAKE_SNAPSHOT_FAILURE = "raft_take_snapshot_failure";
    String RAFT_TAKE_SNAPSHOT_FAILURE_DESC = "Counter to measure the number of failed take snapshot operations in Ratis";

    String RAFT_QUERY = "raft_query";
    String RAFT_QUERY_DESC = "Timer to measure the time taken to query in Ratis";

    String RAFT_TRANSACTION = "raft_transaction";
    String RAFT_TRANSACTION_DESC = "Timer to measure the time taken to transaction a command in Ratis";

}
