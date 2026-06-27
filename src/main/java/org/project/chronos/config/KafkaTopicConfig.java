package org.project.chronos.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.project.chronos.constants.ChronosConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaTopicConfig implements ChronosConstants {

    @Autowired
    private EnvProperty envProperty;

    @Bean
    public KafkaAdmin admin() {
        Map <String, Object> configs = new HashMap <> ();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, envProperty.getProducerBootstrapServers());
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopics chronosTopics() {
        return new NewTopics(getNewTopics());
    }

    private NewTopic[] getNewTopics() {
        List<NewTopic> newTopicList = new ArrayList<>();
        newTopicList.add(TopicBuilder.name(envProperty.getChronosProcessInitiationTopic())
                .partitions(envProperty.getNumberOfPartition())
                .replicas(envProperty.getNumberOfReplica())
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(envProperty.getRetentionPeriodMs()))
                .build());
        newTopicList.add(TopicBuilder.name(envProperty.getChronosProcessCompletionTopic())
                .partitions(envProperty.getNumberOfPartition())
                .replicas(envProperty.getNumberOfReplica())
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(envProperty.getRetentionPeriodMs()))
                .build());
        for (int i = 0; i < envProperty.getChronosProcessRetries(); i++) {
            newTopicList.add(TopicBuilder.name(envProperty.getChronosProcessRetryTopicPrefix().concat(Integer.toString(i)))
                    .partitions(envProperty.getNumberOfPartition())
                    .replicas(envProperty.getNumberOfReplica())
                    .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(envProperty.getRetentionPeriodMs()))
                    .build());
        }

        return newTopicList.toArray(new NewTopic[envProperty.getChronosProcessRetries() + 2]);
    }
}
