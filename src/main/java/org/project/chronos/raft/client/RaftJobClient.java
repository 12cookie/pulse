package org.project.chronos.raft.client;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.util.TimeDuration;
import org.project.chronos.config.EnvProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.project.chronos.raft.server.RaftJobServer.RAFT_GROUP_ID;

@Slf4j
@Getter
@Service
@ConditionalOnProperty(name = "smart.qc.enable.raft", havingValue = "true")
public final class RaftJobClient implements Closeable {

    @Autowired
    private EnvProperty envProperty;

    private volatile RaftClient client;

    @PostConstruct
    public synchronized void init() {

        List<RaftPeer> peers = Arrays.stream(envProperty.getRaftPeers().split(","))
                .map(p -> {
                    int colonIndex = p.indexOf(":");
                    if (colonIndex <= 0 || colonIndex >= p.length() - 1) {
                        throw new IllegalArgumentException(
                                "Invalid raft peer configuration entry (expected '<id>:<address>'): '" + p + "'");
                    }

                    return RaftPeer.newBuilder()
                            .setId(p.substring(0, colonIndex))
                            .setAddress(p.substring(colonIndex + 1))
                            .build();
                })
                .collect(Collectors.toList());

        RaftProperties properties = new RaftProperties();
        RaftGroup raftGroup = RaftGroup.valueOf(RAFT_GROUP_ID, peers);
        RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(envProperty.getRpcClientRequestTimeout(), TimeUnit.MILLISECONDS));

        this.close();
        client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(raftGroup)
                .setClientId(ClientId.randomId())
                .build();
    }

    public RaftClientReply sendCommand(String command) throws IOException {
        try {
            return client.io().send(Message.valueOf(command));
        } catch (Exception e) {
            handleException(e);
            return client.io().send(Message.valueOf(command));

        }
    }

    public RaftClientReply sendQuery(String query) throws IOException {
        try {
            return client.io().sendReadOnly(Message.valueOf(query));
        } catch (Exception e) {
            handleException(e);
            return client.io().sendReadOnly(Message.valueOf(query));
        }
    }

    private void handleException(Exception e) throws IOException {

        if (e instanceof AlreadyClosedException) {

            log.warn("RaftClient AlreadyClosedException encountered: {}", e.getMessage());
            synchronized (this) {
                log.warn("RaftClient is closed, reinitializing...");
                init();
            }

        } else if (e instanceof IOException) {
            log.error("IOException encountered in RaftClient: {}", e.getMessage());
            throw (IOException) e;

        } else {
            log.error("Exception encountered in RaftClient: {}", e.getMessage());
        }
    }

    @PreDestroy
    public void close() {

        RaftClient oldClient = this.client;
        this.client = null;
        if (oldClient != null) {
            try {
                oldClient.close();
            } catch (IOException e) {
                log.warn("Failed to close existing RaftClient during initialization: {}", e.getMessage());
            }
        }
    }
}
