package com.bizseer.hubble.mq.kafka.properties;

import com.bizseer.hubble.common.file.FileUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * kafka 配置
 *
 * @author: xl
 * @date: 2021/7/8
 **/
@Data
@Slf4j
public class KafkaProperties {


    private final static String kafka_producer_config_prefix = "spring.kafka.producer.";
    private final static String kafka_consumer_config_prefix = "spring.kafka.consumer.";

    static Map<String, Object> configAllMap = FileUtil.loadFlattenedYaml();
    static Map<String, Object> configProducerKafkaMap = null;
    static Map<String, Object> configConsumerKafkaMap = null;


    private static ProducerNestedProperties producer = new ProducerNestedProperties();
    private static ConsumerNestedProperties consumer = new ConsumerNestedProperties();
    private static AdminClientNestedProperties admin = new AdminClientNestedProperties();

    static {
        /**过滤kafka配置*/
        configProducerKafkaMap = configAllMap.entrySet().stream().filter(config -> config
            .getKey().startsWith(kafka_producer_config_prefix)).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
        configConsumerKafkaMap = configAllMap.entrySet().stream().filter(config -> config
            .getKey().startsWith(kafka_consumer_config_prefix)).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
        producer.buildProperties();
        consumer.buildProperties();
        admin.buildProperties();
    }


    @Data
    public static class ProducerNestedProperties {


        private String keySerializerProducer;
        private String valueSerializerProducer;

        private String bootstrapServers;
        private Integer lingerMs;
        private Integer batchSize;
        /**
         * values are <code>none</code>, <code>gzip</code>, <code>snappy</code>, <code>lz4</code> or <code>zstd(kafka 2.1+)</code>
         * zstd > lz4 > snappy > gzip
         */
        private String compressType;
        private Integer maxInFlightRequestsPerConnection;
        private String acks;
        private Integer retries;
        private Integer retriesBackoffMs;
        private Integer requestTimeoutMs;
        private HashMap<String, Object> propertiesMap = null;

        public Map<String, Object> buildProperties() {
            if (MapUtils.isNotEmpty(propertiesMap)) {
                return propertiesMap;
            }
            propertiesMap = new HashMap<>();
            //必填参数
            propertiesMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
            propertiesMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getKeySerializerProducer());
            propertiesMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getValueSerializerProducer());
            /////////////////////调优参数/////////////////////
            //最重要的参数之一
            propertiesMap.put(ProducerConfig.LINGER_MS_CONFIG, getLingerMs());
            propertiesMap.put(ProducerConfig.BATCH_SIZE_CONFIG, getBatchSize());
            //压缩参数，降低网络I/O传输开销，增加CPU开销（默认none）
            propertiesMap.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, getCompressType());
            //设置一次只允许producer发送一个消息，防止消息重发时产生乱序 (默认5个)
            propertiesMap.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, getMaxInFlightRequestsPerConnection());
            //1代表producer发送后leader broker仅写入本地日志后，然后发送响应结果给producer，无须等ISR完成 (默认值是1)
            propertiesMap.put(ProducerConfig.ACKS_CONFIG, getAcks());
            //设置重试次数（2.0.0默认0次，2.1.0开始默认是Integer.MAX_VALUE次）
            propertiesMap.put(ProducerConfig.RETRIES_CONFIG, getRetries());
            //两次重试之间会有一段停顿时间（默认是100ms）
            propertiesMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, getRetriesBackoffMs());
            //发送到broker，broker的响应时间上限（默认30秒）
            propertiesMap.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, getRequestTimeoutMs());
            return propertiesMap;
        }

        public String getKeySerializerProducer() {
            return MapUtils.getString(configProducerKafkaMap, kafka_producer_config_prefix + "key-serializer", "org.apache.kafka.common.serialization.StringSerializer");
        }

        public String getValueSerializerProducer() {
            return MapUtils.getString(configProducerKafkaMap, kafka_producer_config_prefix + "value-serializer", "org.apache.kafka.common.serialization.StringSerializer");
        }

        public String getBootstrapServers() {
            return MapUtils.getString(configProducerKafkaMap, kafka_producer_config_prefix + "bootstrap-servers", "127.0.0.1:9200");
        }

        public Integer getBatchSize() {
            return MapUtils.getInteger(configProducerKafkaMap, kafka_producer_config_prefix + "batch-size", 256000);
        }

        public String getCompressType() {
            return MapUtils.getString(configProducerKafkaMap, kafka_producer_config_prefix + ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        }

        public Integer getLingerMs() {
            return MapUtils.getInteger(configProducerKafkaMap, kafka_producer_config_prefix + "properties.linger.ms", 1000);
        }

        public Integer getMaxInFlightRequestsPerConnection() {
            return MapUtils.getInteger(configProducerKafkaMap, kafka_producer_config_prefix + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        }

        public String getAcks() {
            return MapUtils.getString(configProducerKafkaMap, kafka_producer_config_prefix + ProducerConfig.ACKS_CONFIG, "1");
        }

        public Integer getRetries() {
            return MapUtils.getInteger(configProducerKafkaMap, kafka_producer_config_prefix + ProducerConfig.RETRIES_CONFIG, 2);
        }

        public Integer getRetriesBackoffMs() {
            return MapUtils.getInteger(configProducerKafkaMap, kafka_producer_config_prefix + ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        }

        public Integer getRequestTimeoutMs() {
            return MapUtils.getInteger(configProducerKafkaMap, kafka_producer_config_prefix + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        }
    }

    @Data
    public static class ConsumerNestedProperties {


        private String keySerializerProducer;
        private String valueSerializerProducer;
        private String bootstrapServers;
        private String groupId;
        private String autoOffsetReset;
        private Integer sessionTimeout;
        private Integer maxPollIntervalMs;
        private Boolean enableAutoCommit;
        private Integer autoCommitIntervalMs;
        private Integer fetchMaxBytes;
        private Integer maxPollRecords;
        private Integer connectionsIntervalMs;
        private HashMap<String, Object> propertiesMap = null;

        public Map<String, Object> buildProperties() {
            if (MapUtils.isNotEmpty(propertiesMap)) {
                return propertiesMap;
            }
            propertiesMap = new HashMap<>();

            //必填参数
            propertiesMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
            propertiesMap.put(ConsumerConfig.GROUP_ID_CONFIG, getGroupId());
            propertiesMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getKeySerializerProducer());
            propertiesMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getValueSerializerProducer());
            //offset从最早的开始，默认是从最近的开始,该属性指定了消费者在读取一个没有偏移量后者偏移量无效（消费者长时间失效当前的偏移量已经过时并且被删除了）的分区的情况下，应该作何处理，默认值是latest，也就是从最新记录读取数据（消费者启动之后生成的记录），另一个值是earliest，意思是在偏移量无效的情况下，消费者从起始位置开始读取数据。
            propertiesMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, getAutoOffsetReset());
            /////////////////////调优参数/////////////////////
            //consumer处理逻辑的最大时间（默认1分钟）
            propertiesMap.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, getMaxPollIntervalMs());
            //自动确认，真实场景下需要手动确认（默认true）
            propertiesMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, getEnableAutoCommit());
            propertiesMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, getAutoCommitIntervalMs());
            //单次获取数据的最大字节数（默认50M）
            propertiesMap.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, getFetchMaxBytes());
            //每次最多能拉取的消息数（默认500）
            propertiesMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, getMaxPollRecords());
            //kafka会定期关闭（默认9分钟） kafka会定期的关闭空闲Socket连接。默认是9分钟。如果不在乎这些资源开销，推荐把这些参数值为-1，即不关闭这些空闲连接。
            // kafka会定期的关闭空闲Socket连接。默认是9分钟。如果不在乎这些资源开销，推荐把这些参数值为-1，即不关闭这些空闲连接。
            propertiesMap.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, getConnectionsIntervalMs());
            return propertiesMap;
        }

        public String getKeySerializerProducer() {
            return MapUtils.getString(configConsumerKafkaMap, kafka_consumer_config_prefix + "key-deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }

        public String getValueSerializerProducer() {
            return MapUtils.getString(configConsumerKafkaMap, kafka_consumer_config_prefix + "value-deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }

        public String getBootstrapServers() {
            return MapUtils.getString(configConsumerKafkaMap, kafka_consumer_config_prefix + "bootstrap-servers", "127.0.0.1:9200");
        }

        public String getAutoOffsetReset() {
            return MapUtils.getString(configConsumerKafkaMap, kafka_consumer_config_prefix + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }

        public Integer getMaxPollIntervalMs() {
            return MapUtils.getInteger(configConsumerKafkaMap, kafka_consumer_config_prefix + ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60 * 1000);
        }

        public Boolean getEnableAutoCommit() {
            return MapUtils.getBoolean(configConsumerKafkaMap, kafka_consumer_config_prefix + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        }

        public Integer getAutoCommitIntervalMs() {
            return MapUtils.getInteger(configConsumerKafkaMap, kafka_consumer_config_prefix + ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        }

        public Integer getFetchMaxBytes() {
            return MapUtils.getInteger(configConsumerKafkaMap, kafka_consumer_config_prefix + ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 52428800);
        }

        public Integer getMaxPollRecords() {
            return MapUtils.getInteger(configConsumerKafkaMap, kafka_consumer_config_prefix + ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        }

        public Integer getConnectionsIntervalMs() {
            return MapUtils.getInteger(configConsumerKafkaMap, kafka_consumer_config_prefix + ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 9 * 60 * 1000);
        }
    }

    @Data
    public static class AdminClientNestedProperties {
        private String bootstrapServers;
        private HashMap<String, Object> properties = null;

        public Map<String, Object> buildProperties() {

            if (MapUtils.isNotEmpty(properties)) {
                return properties;
            }
            properties = new HashMap<>();

            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
            return properties;
        }


        public String getBootstrapServers() {
            return MapUtils.getString(configProducerKafkaMap, kafka_producer_config_prefix + "bootstrap-servers", "127.0.0.1:9200");
        }
    }

    public Map<String, Object> getProducerProperties() {
        if (producer != null) {
            return producer.buildProperties();

        }
        return null;
    }

    public Map<String, Object> getConsumerProperties() {
        if (consumer != null) {
            return consumer.buildProperties();

        }
        return null;
    }

    public Map<String, Object> getAdminClientProperties() {
        if (admin != null) {
            return admin.buildProperties();

        }
        return null;
    }


}
