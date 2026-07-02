package org.project.chronos.constants;

public interface MetricConstants {

    String timerPostFix = "_timer";

    String gaugePostFix = "_gauge";

    String counterPostfix = "_counter";

    /**
     * RAFT Metrics
     */

    String RAFT_TAKE_SNAPSHOT = "raft_take_snapshot";
    String RAFT_TAKE_SNAPSHOT_DESC = "Timer to measure the time taken to take a snapshot in Ratis";

    String RAFT_TAKE_SNAPSHOT_FAILURE = "raft_take_snapshot_failure";
    String RAFT_TAKE_SNAPSHOT_FAILURE_DESC = "Counter to measure the number of failed take snapshot operations in Ratis";

    String RAFT_QUERY = "raft_query";
    String RAFT_QUERY_DESC = "Timer to measure the time taken to query in Ratis";

    String RAFT_TRANSACTION = "raft_transaction";
    String RAFT_TRANSACTION_DESC = "Timer to measure the time taken to transaction a command in Ratis";
}
