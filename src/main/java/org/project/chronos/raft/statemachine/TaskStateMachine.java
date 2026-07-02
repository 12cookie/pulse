package org.project.chronos.raft.statemachine;

import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.*;
import org.jspecify.annotations.NonNull;
import org.project.chronos.metrics.RaftCustomMetrics;
import org.project.chronos.model.AssignedTask;
import org.project.chronos.model.ChronosTaskMessage;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.project.chronos.constants.MetricConstants.*;
import static org.project.chronos.constants.ChronosConstants.*;
import static org.project.chronos.util.CommonUtil.mapObjectToString;
import static org.project.chronos.util.CommonUtil.mapStringToObject;

/**
 * Raft state machine that stores pending tasks, priority tasks, and assigned task state.
 */
@Slf4j
public class TaskStateMachine extends BaseStateMachine {

    private static final int CURRENT_VERSION = 1;

    private final BlockingQueue<ChronosTaskMessage> pendingTaskQueue = new LinkedBlockingQueue<>();

    private final BlockingQueue<ChronosTaskMessage> priorityTaskQueue = new LinkedBlockingQueue<>();

    private final ConcurrentSkipListMap<Long, AssignedTask> assignedTaskMap = new ConcurrentSkipListMap<>();

    private final ConcurrentHashMap<String, Long> indexMap = new ConcurrentHashMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

    private final RaftCustomMetrics raftCustomMetrics;

    /**
     * Acquire and return a read lock wrapped in an AutoCloseableLock.
     *
     * <p>Use in try-with-resources to automatically release the read lock when done.</p>
     *
     * @return an AutoCloseableLock that holds the read lock
     */
    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    /**
     * Acquire and return a write lock wrapped in an AutoCloseableLock.
     *
     * <p>Use in try-with-resources to automatically release the write lock when done.</p>
     *
     * @return an AutoCloseableLock that holds the write lock
     */
    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    /**
     * Constructs a TaskStateMachine with the provided metrics collector.
     *
     * @param raftCustomMetrics the metrics helper used to record Raft-related metrics
     */
    public TaskStateMachine(RaftCustomMetrics raftCustomMetrics) {
        this.raftCustomMetrics = raftCustomMetrics;
    }

    /**
     * Reset the in-memory state of the state machine.
     *
     * <p>This clears the pending job queue, the assigned job map and resets the last applied
     * term/index so the state machine appears empty.</p>
     */
    void reset() {
        pendingTaskQueue.clear();
        priorityTaskQueue.clear();
        assignedTaskMap.clear();
        indexMap.clear();
        setLastAppliedTermIndex(null);
    }

    /**
     * Initializes the state machine with the given Raft server, group ID, and storage.
     *
     * <p>Initializes internal storage, loads the latest snapshot, registers metrics gauges and
     * transitions the lifecycle to started state.</p>
     *
     * @param server      the RaftServer this state machine is attached to
     * @param groupId     the RaftGroupId for the group
     * @param raftStorage the RaftStorage providing snapshot/replicated storage
     * @throws IOException if snapshot loading or storage initialization fails
     */
    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
        log.debug("Initializing TaskStateMachine...");
        super.initialize(server, groupId, raftStorage);
        storage.init(raftStorage);
        getLifeCycle().startAndTransition(() -> {
            loadSnapshot(storage.getLatestSnapshot());
            raftCustomMetrics.registerStateMachineGauges(this);
            log.debug("TaskStateMachine initialized successfully.");
        });
    }

    /**
     * Reinitialized the state machine by reloading the latest snapshot from storage.
     *
     * <p>Closes current state, loads the snapshot and logs the result.</p>
     *
     * @throws IOException if loading the snapshot fails
     */
    @Override
    public void reinitialize() throws IOException {
        log.debug("Reinitializing TaskStateMachine...");
        close();
        loadSnapshot(storage.getLatestSnapshot());
        log.debug("TaskStateMachine reinitialized successfully.");
    }

    /**
     * Pause the state machine lifecycle.
     *
     * <p>Invokes the base pause logic and transitions lifecycle to PAUSING then PAUSED states.</p>
     */
    @Override
    public void pause() {
        log.debug("Pausing TaskStateMachine...");
        super.pause();
        getLifeCycle().transition(LifeCycle.State.PAUSING);
        getLifeCycle().transition(LifeCycle.State.PAUSED);
        log.debug("TaskStateMachine paused.");
    }

    /**
     * Take a snapshot of the current state and persist it to storage.
     *
     * <p>This creates a snapshot file based on the current last applied term/index and writes
     * the in-memory pending job queue, assigned job map, polled index and stuck polled index.
     * Metrics are recorded for snapshot success/failure and duration.</p>
     *
     * @return the log index associated with the snapshot (last applied index)
     * @throws IOException if writing the snapshot file fails
     */
    @Override
    public long takeSnapshot() throws IOException {
        final BlockingQueue<ChronosTaskMessage> pendingTaskCopy;
        final BlockingQueue<ChronosTaskMessage> priorityTaskCopy;
        final ConcurrentSkipListMap<Long, AssignedTask> assignedTaskCopy;
        final ConcurrentHashMap<String, Long> indexMapCopy;

        long startTime = System.currentTimeMillis();
        final TermIndex last;
        try (AutoCloseableLock _ = readLock()) {
            pendingTaskCopy = new PriorityBlockingQueue<>(pendingTaskQueue);
            priorityTaskCopy = new PriorityBlockingQueue<>(priorityTaskQueue);
            assignedTaskCopy = new ConcurrentSkipListMap<>(assignedTaskMap);
            indexMapCopy = new ConcurrentHashMap<>(indexMap);
            last = getLastAppliedTermIndex();
        }

        final File snapshotFile = storage.getSnapshotFile(last.getTerm(), last.getIndex());
        log.info("Taking a snapshot to file {}", snapshotFile);
        try (ObjectOutputStream out = new ObjectOutputStream(
                new BufferedOutputStream(
                        FileUtils.newOutputStream(snapshotFile)))) {
            out.writeInt(CURRENT_VERSION);
            log.info("Snapshot version during write: {}", CURRENT_VERSION);
            out.writeObject(pendingTaskCopy);
            out.writeObject(priorityTaskCopy);
            out.writeObject(assignedTaskCopy);
            out.writeObject(indexMapCopy);
        } catch (IOException ioe) {
            raftCustomMetrics.count(RAFT_TAKE_SNAPSHOT_FAILURE, RAFT_TAKE_SNAPSHOT_FAILURE_DESC);
            log.error("Failed to write snapshot file {}, last applied index = {}", snapshotFile, last);
            throw ioe;
        }

        final MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
        final FileInfo info = new FileInfo(snapshotFile.toPath(), md5);
        storage.updateLatestSnapshot(new SingleFileSnapshotInfo(info, last));
        log.info("Snapshot taken successfully: {}, last applied index = {}", snapshotFile, last);
        raftCustomMetrics.recordTime(RAFT_TAKE_SNAPSHOT, RAFT_TAKE_SNAPSHOT_DESC, startTime);
        return last.getIndex();
    }

    /**
     * Load and apply the contents of the provided snapshot to restore state.
     *
     * <p>If the snapshot is null or the underlying file is missing the method returns without changes.
     * When a valid snapshot is present, the snapshot file is verified (MD5 when available), the in-memory
     * state is cleared and replaced with the values read from the snapshot, and the last applied term/index
     * is set accordingly.</p>
     *
     * @param snapshot the SingleFileSnapshotInfo containing the snapshot to load
     * @throws IOException if reading the snapshot file fails or verifying MD5 fails
     */
    public void loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
        if (snapshot == null) {
            log.warn("The snapshot info is null.");
            return;
        }

        final File snapshotFile = snapshot.getFile().getPath().toFile();
        if (!snapshotFile.exists()) {
            log.warn("The snapshot file {} does not exist for snapshot {}", snapshotFile, snapshot);
            return;
        }

        final MD5Hash md5 = snapshot.getFile().getFileDigest();
        if (md5 != null) {
            MD5FileUtil.verifySavedMD5(snapshotFile, md5);
        }

        final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
        try (AutoCloseableLock ignored = writeLock();
             ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(
                     FileUtils.newInputStream(snapshotFile)))) {
            reset();
            setLastAppliedTermIndex(last);
            int version = in.readInt();
            log.info("Snapshot version during read: {}", version);
            pendingTaskQueue.addAll(JavaUtils.cast(in.readObject()));
            priorityTaskQueue.addAll(JavaUtils.cast(in.readObject()));
            assignedTaskMap.putAll(JavaUtils.cast(in.readObject()));
            indexMap.putAll(JavaUtils.cast(in.readObject()));
            log.info("Snapshot loaded successfully from file {}, last applied index = {}", snapshotFile, last);
        } catch (ClassNotFoundException | IOException e) {
            log.error("Failed to load snapshot file {}, last applied index = {}, error: {}", snapshotFile, last, e.getMessage());
            log.debug("Stack trace: ", e);
            throw new IllegalStateException("Failed to load " + snapshot, e);
        }
    }

    /**
     * Return the underlying StateMachineStorage used by this state machine.
     *
     * @return the StateMachineStorage instance
     */
    @Override
    public StateMachineStorage getStateMachineStorage() {
        return storage;
    }

    /**
     * Handle a read-only query against the state machine.
     *
     * <p>Supported query commands are parsed from the provided message content. This method
     * executes under a read lock, records query metrics and returns a CompletableFuture containing
     * a Message with the result string.</p>
     *
     * @param request the Message containing the UTF-8 encoded query string
     * @return a CompletableFuture that completes with a Message containing the query response
     */
    @Override
    public CompletableFuture<Message> query(Message request) {
        long startTime = System.currentTimeMillis();
        String receivedQuery = request.getContent().toStringUtf8();
        String[] parts = receivedQuery.split(COLON, 2);
        CompletableFuture<Message> queryResult;
        try (AutoCloseableLock ignored = readLock()) {
            log.debug("Received query: {}", receivedQuery);
            switch (parts[0]) {
                case GET_PENDING_TASK_QUEUE_SIZE -> queryResult = getQueryResult(pendingTaskQueue);
                case GET_PRIORITY_TASK_QUEUE_SIZE -> queryResult = getQueryResult(priorityTaskQueue);
                case GET_ASSIGNED_TASK_FROM_MAP -> queryResult = getAssignedTaskFromMap(parts[1]);
                default -> queryResult = CompletableFuture.completedFuture(Message.EMPTY);
            }

            return queryResult;
        } finally {
            raftCustomMetrics.recordTime(RAFT_QUERY, RAFT_QUERY_DESC, startTime);
        }
    }

    private <T> CompletableFuture<Message> getQueryResult(Collection<T> collection) {
        return CompletableFuture.completedFuture(Message.valueOf(Integer.toString(collection.size())));
    }

    /**
     * Apply a state-changing transaction (command) to the state machine.
     *
     * <p>The command is parsed and executed under a write lock. Supported actions include adding jobs,
     * updating indices, moving jobs between queues and maps, and handling expired jobs. Metrics for
     * transaction success/failure and timing are recorded. The method returns a CompletableFuture that
     * completes with a Message containing the result string.</p>
     *
     * @param transactionContext the TransactionContext containing the log entry and other transaction metadata
     * @return a CompletableFuture that completes with a Message containing the action result
     */
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext transactionContext) {
        long startTime = System.currentTimeMillis();
        final RaftProtos.LogEntryProto entry = transactionContext.getLogEntry();
        String command = entry.getStateMachineLogEntry().getLogData().toStringUtf8();
        CompletableFuture<Message> trxResult;
        try (AutoCloseableLock _ = writeLock()) {
            log.debug("Received command: {}", command);
            String[] parts = command.split(COLON, 2);
            String transactionName = parts[0];
            switch (transactionName) {
                case ADD_TASK_TO_QUEUE -> trxResult = addTaskToQueue(parts[1]);
                case ADD_PRIORITY_TASK_TO_QUEUE -> trxResult = addPriorityTaskToQueue(parts[1]);
                case GET_PENDING_TASK -> trxResult = getPendingTask(parts[1]);
                case REMOVE_TASK_FROM_MAP -> trxResult = removeTaskFromMap(Long.valueOf(parts[1]));
                case HANDLE_EXPIRED_TASKS -> trxResult = getExpiredTask(Long.parseLong(parts[1]));
                default -> trxResult = CompletableFuture.completedFuture(Message.EMPTY);
            }

            return trxResult;
        } finally {
            raftCustomMetrics.recordTime(RAFT_TRANSACTION, RAFT_TRANSACTION_DESC, startTime);
        }
    }

    private @NonNull CompletableFuture<Message> addTaskToQueue(String task) {
        ChronosTaskMessage chronosTaskMessage = mapStringToObject(task, ChronosTaskMessage.class);
        pendingTaskQueue.add(chronosTaskMessage);
        log.debug("Size of pending queue: {}", pendingTaskQueue.size());
        return CompletableFuture.completedFuture(Message.valueOf(TASK_ADDED_SUCCESSFULLY));
    }

    private @NonNull CompletableFuture<Message> addPriorityTaskToQueue(String task) {
        ChronosTaskMessage chronosTaskMessage = mapStringToObject(task, ChronosTaskMessage.class);
        priorityTaskQueue.add(chronosTaskMessage);
        log.debug("Size of priority queue: {}", priorityTaskQueue.size());
        return CompletableFuture.completedFuture(Message.valueOf(TASK_ADDED_SUCCESSFULLY));
    }

    private @NonNull CompletableFuture<Message> getPendingTask(String taskExecutorId) {
        long startTime = System.currentTimeMillis();
        ChronosTaskMessage task = priorityTaskQueue.poll();

        if (Objects.isNull(task)) {
            task = pendingTaskQueue.poll();
        }

        if (Objects.isNull(task)) {
            log.debug("No pending task available");
            return CompletableFuture.completedFuture(Message.valueOf(NO_PENDING_JOB));
        }

        AssignedTask assignedTask = AssignedTask.builder()
                .taskExecutorClientId(taskExecutorId)
                .task(task)
                .build();
        assignedTaskMap.put(startTime, assignedTask);
        indexMap.put(assignedTask.getTask().getTaskId(), startTime);
        String serializedTask = mapObjectToString(task);
        log.debug("Pending task: {}", serializedTask);
        return CompletableFuture.completedFuture(Message.valueOf(serializedTask));
    }

    private @NonNull CompletableFuture<Message> removeTaskFromMap(Long key) {
        indexMap.remove(assignedTaskMap.get(key).getTask().getTaskId());
        assignedTaskMap.remove(key);
        log.debug("Size of assigned map: {}", assignedTaskMap.size());
        return CompletableFuture.completedFuture(Message.valueOf(TASK_REMOVED_SUCCESSFULLY));
    }

    private @NonNull CompletableFuture<Message> getExpiredTask(long taskTimeoutMs) {
        long expiredKey = System.currentTimeMillis() - taskTimeoutMs;
        NavigableMap<Long, AssignedTask> expired = assignedTaskMap.headMap(expiredKey);
        List<AssignedTask> expiredTasks = new ArrayList<>(expired.values());
        expiredTasks.forEach(t -> indexMap.remove(t.getTask().getTaskId()));
        expired.clear();
        return CompletableFuture.completedFuture(Message.valueOf(mapObjectToString(expiredTasks)));
    }

    private @NonNull CompletableFuture<Message> getAssignedTaskFromMap(String taskId) {
        AssignedTask task = assignedTaskMap.get(indexMap.get(taskId));
        if (Objects.isNull(task)) {
            return CompletableFuture.completedFuture(Message.valueOf(NO_TASK_FOUND_IN_MAP));
        }

        String assignedTask = mapObjectToString(new AbstractMap.SimpleEntry<>(indexMap.get(taskId), task));
        log.debug("Response on command {}: {}", GET_ASSIGNED_TASK_FROM_MAP, assignedTask);
        return CompletableFuture.completedFuture(Message.valueOf(assignedTask));
    }

    /**
     * Close the state machine and release resources.
     *
     * <p>Current implementation resets in-memory state; override to close other resources if needed.</p>
     */
    @Override
    public void close() {
        reset();
    }

    /**
     * Get the current number of jobs waiting in the pending queue.
     *
     * @return the size of the pending job queue
     */
    public int getPendingQueueSize() {
        return this.pendingTaskQueue.size();
    }

    public int getPriorityQueueSize() {
        return this.priorityTaskQueue.size();
    }

    /**
     * Get the current number of jobs in the assigned (processing) map.
     *
     * @return the size of the assigned jobs map
     */
    public int getProcessingTaskMapSize() {
        return this.assignedTaskMap.size();
    }
}
