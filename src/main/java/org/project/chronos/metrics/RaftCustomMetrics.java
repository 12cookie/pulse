package org.project.chronos.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.project.chronos.raft.statemachine.TaskStateMachine;
import org.springframework.stereotype.Component;

import static org.project.chronos.constants.MetricConstants.gaugePostFix;

@Component
public class RaftCustomMetrics extends AbstractMetrics {

    private final MeterRegistry meterRegistry;

    public RaftCustomMetrics(MeterRegistry meterRegistry) {
        super(meterRegistry);
        this.meterRegistry = meterRegistry;
    }

    public void registerStateMachineGauges(TaskStateMachine stateMachine) {

        Gauge.builder("raft_pending_tasks".concat(gaugePostFix), stateMachine::getPendingQueueSize)
                .description("Number of pending tasks in the task State Machine")
                .register(meterRegistry);
        Gauge.builder("raft_priority_tasks".concat(gaugePostFix), stateMachine::getPriorityQueueSize)
                .description("Number of priority tasks in the task State Machine")
                .register(meterRegistry);
        Gauge.builder("raft_processing_tasks".concat(gaugePostFix), stateMachine::getProcessingTaskMapSize)
                .description("Number of processing tasks in the task State Machine")
                .register(meterRegistry);
    }
}
