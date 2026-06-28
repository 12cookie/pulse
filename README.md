# Chronos

Chronos is a Spring Boot task orchestration service built around Kafka, gRPC, and Apache Ratis.

It accepts task messages from Kafka, keeps task state in a Raft-backed state machine, assigns work to task executors over gRPC, and tracks task completion, retries, and expiration.

## What it does

Chronos is designed for distributed task execution where:

- Kafka is the ingestion layer for new work
- Raft is the replicated source of truth for queue and assignment state
- gRPC is the executor-facing API used to fetch tasks and submit results
- retries and delayed re-processing are handled through Kafka retry topics

The service supports two task lanes:

- normal tasks
- priority tasks

## High-level flow

1. A producer publishes a `ChronosTaskMessage` to the initiation or priority Kafka topic.
2. Chronos consumes the message and writes it into the Raft state machine.
3. A task executor calls the gRPC `getTask` method with its executor client ID.
4. Chronos returns the next available task, if one exists.
5. The executor runs the task and calls `submitTaskResult`.
6. Chronos stores the result, republishes failed or expired tasks to retry handling, and removes the assignment from the map.

## Project structure

- `src/main/java/org/project/chronos/ChronosApplication.java` - Spring Boot entry point
- `src/main/java/org/project/chronos/grpc` - gRPC service exposed to executors
- `src/main/java/org/project/chronos/kafka` - Kafka producer and consumers
- `src/main/java/org/project/chronos/raft` - Raft client, server, and state machine
- `src/main/java/org/project/chronos/model` - task and result payload models
- `src/main/proto/ChronosExecutor.proto` - gRPC contract

## Runtime dependencies

Chronos expects these services to be available:

- Kafka
- A Raft peer set

The default configuration in `src/main/resources/application.properties` points Kafka to `localhost:9092`.

## Configuration

The most important properties are:

### Kafka

- `spring.kafka.consumer.bootstrap-servers` - consumer bootstrap server list
- `spring.kafka.producer.bootstrap-servers` - producer bootstrap server list
- `spring.kafka.listener.concurrency` - listener concurrency
- `spring.kafka.consumer.group-id` - consumer group id
- `chronos.process.initiation.topic` - normal task topic, default `CHRONOS.PROCESS.INITIATION.TOPIC`
- `chronos.process.priority.topic` - priority task topic, default `CHRONOS.PROCESS.PRIORITY.TOPIC`
- `chronos.process.retry.topic` - retry topic prefix, default `CHRONOS.PROCESS.RETRY.TOPIC-`
- `chronos.process.completion.topic` - completion topic, default `CHRONOS.PROCESS.COMPLETION.TOPIC`

### Queue and retry control

- `raft.queue.size.max` - pause Kafka intake when Raft queue reaches this size
- `raft.queue.size.min` - resume Kafka intake when queue drops below this size
- `chronos.process.retry.total` - retry count limit
- `chronos.process.retry.interval.ms` - retry delay window
- `chronos.scheduler.thread.pool.size` - scheduler pool used for delayed retry handling

### Raft

- `enable.raft` - enables Raft-backed task flow
- `raft.peers` - comma-separated peer list in the form `id:host:port`
- `raft.logs.storage.path` - storage directory prefix for local Raft logs and snapshots
- `task.timeout.ms` - expiration window for assigned tasks
- `raft.client.rpc.retry.count` - Raft client retry count
- `raft.client.rpc.retry.interval` - Raft client retry sleep interval in seconds
- `raft.client.rpc.request.timeout.ms` - Raft client request timeout in milliseconds

### Node identity

Chronos uses the `HOSTNAME` environment variable to determine which Raft peer the process should bind to.

The default lookup expects a pod-style name such as:

- `chronos-0`
- `chronos-1`
- `chronos-2`

The last numeric suffix is mapped to Raft peer ids like `n0`, `n1`, `n2`.

## Default topics

Unless overridden, the service uses these Kafka topics:

- `CHRONOS.PROCESS.INITIATION.TOPIC`
- `CHRONOS.PROCESS.PRIORITY.TOPIC`
- `CHRONOS.PROCESS.RETRY.TOPIC-.*`
- `CHRONOS.PROCESS.COMPLETION.TOPIC`

The retry listener uses a topic pattern, so retry messages are expected on topics that match the prefix plus a suffix.

## gRPC API

The executor-facing service is defined in `src/main/proto/ChronosExecutor.proto`.

### `getTask(GetTaskRequest) returns (ChronosTask)`

Request:

```proto
message GetTaskRequest {
  string taskExecutorClientId = 1;
}
```

Response:

```proto
message ChronosTask {
  bool taskAvailable = 1;
  string taskId = 2;
  string taskData = 3;
}
```

Behavior:

- returns a task when one is available for the executor
- returns `taskAvailable = false` when the queue is empty
- rejects blank executor IDs with `INVALID_ARGUMENT`

### `submitTaskResult(ResultSubmissionRequest) returns (ResultAcknowledgment)`

Request:

```proto
message ResultSubmissionRequest {
  string taskId = 1;
  bool success = 2;
  string resultData = 3;
  string errorMessage = 4;
}
```

Response:

```proto
message ResultAcknowledgment {
  string message = 1;
}
```

Behavior:

- if the task id is unknown, Chronos returns an acknowledgment explaining that no task was found
- on success, Chronos publishes a completed-task result
- on failure, Chronos publishes the task into the failed-task flow for retry handling
- the assignment is removed from the in-memory map after submission

## Task message model

`ChronosTaskMessage` is the internal task payload used by Kafka and Raft:

```java
public class ChronosTaskMessage implements Serializable {
    @JsonCreator
    public ChronosTaskMessage() {
        this.errorMessage = new HashMap<>();
        this.resubmissionCount = 0;
    }

    @JsonProperty(required = true)
    private String taskId;

    @JsonProperty(required = true)
    private ChronosTask chronosTask;

    private Map<Integer, String> errorMessage;
    private int resubmissionCount;
}
```

The result message tracked by the system is:

```java
public class ChronosResultMessage {
    private String taskId;
    private boolean success;
    private String taskExecutorId;
    private String taskResult;
    private Map<Integer, String> errorMessage;
}
```

## How task execution works

Chronos tracks three pieces of state in the Raft state machine:

- pending task queue
- priority task queue
- assigned task map

When a task executor asks for work, the service:

1. validates the executor client id
2. queries the Raft state machine for the next task
3. converts the internal task model into the gRPC response payload

When an executor submits a result:

1. Chronos looks up the assignment by task id
2. if the task failed, it republishes it to the failed-task path
3. if the task succeeded, it publishes a completion result
4. it removes the assignment from the map

Expired tasks are checked on a schedule by the Raft task flow handler and moved into the failure flow.

## Notes for local development

- Keep Kafka running before starting Chronos.
- Make sure the `raft.peers` list matches the node you are running.
- Set `HOSTNAME` so the process resolves to the correct peer id.
- If you run a single-node setup, make the peer list and the hostname consistent with that single node.
