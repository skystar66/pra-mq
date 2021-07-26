package com.bizseer.hubble.mq.kafka;

import com.bizseer.hubble.mq.MQTopic;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author: xl
 * @date: 2021/7/8
 **/
public class KafkaAdmin {


    private static final Logger logger = LoggerFactory.getLogger(KafkaAdmin.class);
    private final Map<String, Object> config;
    private long operationTimeout = 30;
    private long closeTimeout = 10;

    public KafkaAdmin(Map<String, Object> config) {
        this.config = config;
    }


    /**
     * 添加消息队列
     *
     * @param topic
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    public void addTopicIfNeeded(MQTopic topic) {
        NewTopic newTopic = new NewTopic(topic.getTopic(), topic.getNumPartitions(), (short) 1);
        checkAndAddTopic(Collections.singletonList(newTopic));
    }

    /**
     * 检查 添加消息队列
     *
     * @param newTopics
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    private void checkAndAddTopic(List<NewTopic> newTopics) {
        if (newTopics != null && !newTopics.isEmpty()) {
            AdminClient adminClient = null;
            try {
                adminClient = AdminClient.create(config);
            } catch (Exception e) {
                logger.error("Could not create adminClient, cause: {}", e);
                throw new IllegalStateException("Could not create adminClient", e);
            }
            try {
                if (adminClient != null) {
                    addTopicsIfNeeded(adminClient, newTopics);
                }
            } catch (Exception e) {
                logger.error("Init topics failure, cause: {}", e);
            } finally {
                if (adminClient != null) {
                    adminClient.close(closeTimeout, TimeUnit.SECONDS);
                }
            }
        }
    }


    private void addTopicsIfNeeded(AdminClient adminClient, List<NewTopic> newTopics) {
        if (newTopics.size() > 0) {
            Map<String, NewTopic> topicNameToTopic = new HashMap<>();
            newTopics.forEach(topic -> topicNameToTopic.compute(topic.name(), (k, v) -> topic));
            List<String> topicNames = newTopics.stream()
                .map(NewTopic::name)
                .collect(Collectors.toList());
            /**获取topic 详情{TopicPartitionInfo,...}*/
            DescribeTopicsResult topicInfo = adminClient.describeTopics(topicNames);
            List<NewTopic> topicToAdd = new ArrayList<>();
            Map<String, NewPartitions> topicToModify = checkPartitions(topicNameToTopic, topicInfo, topicToAdd);
            if (topicToAdd.size() > 0) {
                logger.info("Found some topics need creation to broker, topics: {} size: {}", topicToAdd, topicToAdd.size());
                addTopics(adminClient, topicToAdd);
            }
            if (topicToModify.size() > 0) {
                logger.info("Found some topics need creation partition count, topics: {} size: {}", topicToModify, topicToModify.size());
                modifyTopics(adminClient, topicToModify);
            }

        }
    }


    /**
     * 检查当前topic是否是新增或是修改
     *
     * @param topicNameToTopic
     * @param topicInfo
     * @param topicsToAdd
     * @return: java.util.Map<java.lang.String, org.apache.kafka.clients.admin.NewPartitions>
     * @author: xl
     * @date: 2021/7/8
     **/
    private Map<String, NewPartitions> checkPartitions(Map<String, NewTopic> topicNameToTopic,
                                                       DescribeTopicsResult topicInfo, List<NewTopic> topicsToAdd) {
        Map<String, NewPartitions> topicsToModify = new HashMap<>();
        topicInfo.values().forEach((name, future) -> {
            NewTopic topic = topicNameToTopic.get(name);
            if (topic != null) {
                try {
                    TopicDescription td = future.get(this.operationTimeout, TimeUnit.SECONDS);
                    if (topic.numPartitions() < td.partitions().size()) {
                        logger.info("Topic {} exist but has different partition count, now {} not {}",
                            name, td.partitions().size(), topic.numPartitions());
                    } else if (topic.numPartitions() > td.partitions().size()) {
                        logger.info("Topic {} exist but has different partition count, now {} not {}, increasing if broker support it",
                            name, td.partitions().size(), topic.numPartitions());
                        topicsToModify.put(name, NewPartitions.increaseTo(topic.numPartitions()));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    topicsToAdd.add(topic);
                } catch (TimeoutException e) {
                    throw new KafkaException("Time out waiting for get exist topic");
                }
            }
        });
        return topicsToModify;
    }


    /**
     * 添加topic信息
     *
     * @param adminClient
     * @param topicToAdd
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    private void addTopics(AdminClient adminClient, List<NewTopic> topicToAdd) {
        if (topicToAdd.size() > 0) {
            logger.info("addTopics topics: {}", topicToAdd);
            CreateTopicsResult topicsResult = adminClient.createTopics(topicToAdd);
            try {
                topicsResult.all().get(this.operationTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while waiting for topics creation results, cause: {}", e);
            } catch (ExecutionException e) {
                logger.error("Failed to create topics, cause: {}", e);
                if (!(e.getCause() instanceof UnsupportedClassVersionError)) {
                    throw new KafkaException("Failed to create topics", e);
                }
            } catch (TimeoutException e) {
                throw new KafkaException("Timed out waiting for create topics results", e);
            }
        }
    }

    /**
     * 修改topic 信息 ，TopicPartitionInfo。。。
     *
     * @param adminClient
     * @param topicsToModify
     * @return: void
     * @author: xl
     * @date: 2021/7/8
     **/
    private void modifyTopics(AdminClient adminClient, Map<String, NewPartitions> topicsToModify) {
        if (topicsToModify.size() > 0) {
            CreatePartitionsResult partitionsResult = adminClient.createPartitions(topicsToModify);
            try {
                partitionsResult.all().get(this.operationTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while waiting for partition creation results, cause: {}", e);
            } catch (ExecutionException e) {
                logger.error("Failed to create partitions, cause: {}", e);
                if (!(e.getCause() instanceof UnsupportedClassVersionError)) {
                    throw new KafkaException("Failed to create partitions", e);
                }
            } catch (TimeoutException e) {
                throw new KafkaException("Timed out waiting for create partitions results", e);
            }
        }
    }


}
