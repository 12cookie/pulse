package org.project.chronos.raft.server;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.TimeDuration;
import org.jspecify.annotations.NonNull;
import org.project.chronos.config.EnvProperty;
import org.project.chronos.metrics.RaftCustomMetrics;
import org.project.chronos.raft.statemachine.TaskStateMachine;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.project.chronos.constants.ChronosConstants.*;

/**
 * Configures and starts the embedded Raft server for Chronos task coordination.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "enable.raft", havingValue = "true")
public class RaftTaskServer {

    public static RaftGroupId RAFT_GROUP_ID = RaftGroupId.valueOf(UUID.nameUUIDFromBytes(RAFT_GROUP.getBytes()));

    private final RaftServer raftServer;

    private final RaftGroup raftGroup;

    /**
     * Build and start the Raft server using the configured peers and storage settings.
     *
     * @param envProperty application and Raft configuration
     * @param raftCustomMetrics metrics registry for Raft instrumentation
     * @throws IOException if the server cannot be initialized or started
     */
    public RaftTaskServer(EnvProperty envProperty, RaftCustomMetrics raftCustomMetrics) throws IOException {
        List<RaftPeer> peers = getRaftPeers(envProperty);
        this.raftGroup = RaftGroup.valueOf(RAFT_GROUP_ID, peers);
        String podName = System.getenv().getOrDefault(HOSTNAME, DEFAULT_POD);
        if (podName == null) {
            throw new IllegalStateException("Pod name is required for raft server");
        }

        String serverId = PEER_PREFIX.concat(podName.substring(podName.lastIndexOf(HYPHEN) + 1));
        RaftPeerId currentPeerId = RaftPeerId.valueOf(serverId);
        RaftPeer currentPeer = getCurrentPeer(peers, currentPeerId);

        RaftProperties properties = new RaftProperties();
        File storageDir = new File(envProperty.getStoragePath().concat(currentPeer.getId().toString()));
        if (envProperty.isDirectoryDeletionEnabled() && storageDir.exists()) {
            clearRaftStorage(storageDir);
        }

        int port = NetUtils.createSocketAddr(currentPeer.getAddress()).getPort();
        GrpcConfigKeys.Server.setPort(properties, port);
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
        RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
        RaftServerConfigKeys.Read.setTimeout(properties, TimeDuration.valueOf(envProperty.getReadTimeout(), TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, envProperty.isAutoTriggerEnabled());
        RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, envProperty.getAutoTriggerThreshold());
        RaftServerConfigKeys.Snapshot.setRetentionFileNum(properties, envProperty.getSnapshotRetentionFileNum());
        RaftServerConfigKeys.Log.setPurgeGap(properties, envProperty.getLogPurgeGap());
        RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, envProperty.isLogPurgeUptoSnapshotEnabled());
        RaftServerConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(envProperty.getRpcRequestTimeout(), TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(envProperty.getRpcRequestMinTimeout(), TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(envProperty.getRpcRequestMaxTimeout(), TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.LeaderElection.setLeaderStepDownWaitTime(properties, TimeDuration.valueOf(envProperty.getLeaderStepDownWaitTime(), TimeUnit.MILLISECONDS));
        this.raftServer = RaftServer.newBuilder()
                .setGroup(raftGroup)
                .setProperties(properties)
                .setServerId(currentPeerId)
                .setStateMachine(new TaskStateMachine(raftCustomMetrics))
                .setOption(RaftStorage.StartupOption.RECOVER)
                .build();
        try {
            raftServer.start();
        } catch (Exception ex) {
            log.info("Exception while starting raft server", ex);
            throw new RuntimeException(ex);
        }

        log.info("Raft server {} started at {}", serverId, currentPeerId);
    }

    /**
     * Stop the Raft server during application shutdown.
     *
     * @throws IOException if closing the server fails
     */
    @PreDestroy
    public void stop() throws IOException {
        if (raftServer != null) {
            raftServer.close();
        }
    }

    /**
     * Expose the constructed Raft server as a Spring bean.
     *
     * @return the configured Raft server instance
     */
    @Bean
    public RaftServer raftServer() {
        return this.raftServer;
    }

    /**
     * Expose the Raft group as a Spring bean.
     *
     * @return the configured Raft group
     */
    @Bean
    public RaftGroup raftGroup() {
        return this.raftGroup;
    }

    private static void clearRaftStorage(File storageDir) {
        try (var paths = Files.walk(storageDir.toPath())) {
            paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(t -> {
                if (!t.delete()) {
                    log.warn("Failed to delete file: {}", t.getAbsolutePath());
                }
            });
            log.warn("Raft storage folder has been cleared");

        } catch (Exception e) {
            log.warn("Error while deleting the raft storage ", e);
        }
    }

    private static @NonNull RaftPeer getCurrentPeer(List<RaftPeer> peers, RaftPeerId currentPeerId) {
        return peers.stream().filter(p ->
                        p.getId().equals(currentPeerId))
                .findFirst()
                .orElseThrow(() ->
                        new IllegalArgumentException("Current server ID not found in peers list"));
    }

    private static @NonNull List<RaftPeer> getRaftPeers(EnvProperty envProperty) {
        return Arrays.stream(envProperty.getRaftPeers().split(COMMA))
                .map(p -> RaftPeer.newBuilder()
                        .setId(p.substring(0, p.indexOf(COLON)))
                        .setAddress(p.substring(p.indexOf(COLON) + 1))
                        .build())
                .collect(Collectors.toList());
    }
}
