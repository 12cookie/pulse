package org.project.chronos.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class EnvProperty {

    @Value("${raft.queue.size.max:1000}")
    private int maxRaftQueueSize;

    @Value("${raft.queue.size.min:100}")
    private int minRaftQueueSize;

    /**
     * Kafka properties
     */
    @Value("${app.kafka.topic.replication:1}")
    private int numberOfReplica;

    @Value("${app.kafka.topic.retention.ms:86400000}")
    private int retentionPeriodMs;

    @Value("${app.kafka.topic.partition:3}")
    private int numberOfPartition;

    @Value("${spring.kafka.listener.concurrency:3}")
    private int kafkaListenerConcurrency;

    @Value("${spring.kafka.consumer.group-id:chronos-consumer-group}")
    private String kafkaGroupId;

    @Value("${pod.instance.name:chronos}")
    private String podInstanceName;

    @Value("${kafka.max.poll.records:10}")
    private String kafkaMaxPollRecords;

    @Value("${kafka.max.poll.interval.ms:300000}")
    private String kafkaMaxPollInterval;

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String consumerBootstrapServers;

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String producerBootstrapServers;

    @Value("${spring.kafka.producer.linger.ms:100}")
    private int producerLingerMs;

    @Value("${spring.kafka.producer.batch.size:16384}")
    private int producerBatchSize;

    @Value("${spring.kafka.producer.retries:3}")
    private int producerRetries;

    @Value("${spring.kafka.producer.max.in.flight.requests:1}")
    private String producerMaxInFlightRequests;

    @Value("${spring.kafka.producer.retries:3}")
    private int producerRetriesCount;

    @Value("${spring.kafka.producer.request.timeout.ms:15000}")
    private int producerRequestTimeoutMs;

    @Value("${spring.kafka.producer.retry.backoff.ms:15000}")
    private int producerRetryBackoffMs;

    @Value("${spring.kafka.producer.delivery.timeout.ms:120000}")
    private int producerDeliveryTimeoutMs;

    @Value("${chronos.process.initiation.topic:CHRONOS.PROCESS.INITIATION.TOPIC}")
    private String chronosProcessInitiationTopic;

    @Value("${chronos.process.retry.topic:CHRONOS.PROCESS.RETRY.TOPIC-}")
    private String chronosProcessRetryTopicPrefix;

    @Value("${chronos.process.retry.total:1}")
    private int chronosProcessRetries;

    @Value("${chronos.process.retry.interval.ms:10000}")
    private int chronosProcessRetryIntervalMs;

    @Value("${chronos.process.completion.topic:CHRONOS.PROCESS.COMPLETION.TOPIC}")
    private String chronosProcessCompletionTopic;

    /**
     * Raft properties
     */
    @Value("${raft.peers:n0:localhost:6000,n1:localhost:6001,n2:localhost:6002}")
    private String raftPeers;

    @Value("${task.timeout.ms:10000}")
    private long taskTimeoutMs;

    @Value("${raft.logs.storage.path:./}")
    private String storagePath;

    @Value("${raft.client.rpc.retry.count:3}")
    private int rpcClientRetryCount;

    @Value("${raft.client.rpc.retry.interval:1}")
    private int rpcClientRetryInterval;

    @Value("${raft.client.rpc.request.timeout.ms:3000}")
    private int rpcClientRequestTimeout;

    @Value("${raft.server.auto-trigger.snapshots.enabled:true}")
    private boolean autoTriggerEnabled;

    @Value("${raft.server.auto-trigger.snapshots.threshold:50}")
    private long autoTriggerThreshold;

    @Value("${raft.server.log.purge.gap:10}")
    private int logPurgeGap;

    @Value("${raft.server.log.purge.upto.snapshot.index:true}")
    private boolean logPurgeUptoSnapshotEnabled;

    @Value("${raft.server.rpc.request.timeout.ms:3000}")
    private int rpcRequestTimeout;

    @Value("${raft.server.rpc.request.timeout.min.ms:3000}")
    private int rpcRequestMinTimeout;

    @Value("${raft.server.rpc.request.timeout.max.ms:5000}")
    private int rpcRequestMaxTimeout;

    @Value("${raft.server.read.timeout.ms:60000}")
    private int readTimeout;

    @Value("${raft.server.snapshot.retention.file.num:2}")
    private int snapshotRetentionFileNum;

    @Value("${raft.server.leaderelection.leader.step-down.wait-time.ms:10000}")
    private int leaderStepDownWaitTime;

    @Value("${raft.storage.directory.delete.on-startup:false}")
    private boolean directoryDeletionEnabled;

}
