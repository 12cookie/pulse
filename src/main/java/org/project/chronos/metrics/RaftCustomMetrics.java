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

        Gauge.builder("raft_pending_jobs".concat(gaugePostFix), stateMachine::getPendingQueueSize)
                .description("Number of pending jobs in the Job State Machine")
                .register(meterRegistry);
        Gauge.builder("raft_processing_jobs".concat(gaugePostFix), stateMachine::getProcessingQueueSize)
                .description("Number of processing jobs in the Job State Machine")
                .register(meterRegistry);
    }
}
