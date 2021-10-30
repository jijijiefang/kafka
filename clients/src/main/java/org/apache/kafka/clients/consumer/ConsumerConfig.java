/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * The consumer configuration keys
 */
public class ConsumerConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS
     * THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    /**
     * <code>group.id</code> 消费组名称
     */
    public static final String GROUP_ID_CONFIG = "group.id";
    private static final String GROUP_ID_DOC = "A unique string that identifies the consumer group this consumer belongs to. This property is required if the consumer uses either the group management functionality by using <code>subscribe(topic)</code> or the Kafka-based offset management strategy.";

    /** <code>max.poll.records</code> 每一次poll方法调用拉取的最大消息条数，默认为500*/
    public static final String MAX_POLL_RECORDS_CONFIG = "max.poll.records";
    private static final String MAX_POLL_RECORDS_DOC = "The maximum number of records returned in a single call to poll().";

    /**
     * <code>session.timeout.ms</code> 消费者与broker的心跳超时时间,默认10s，broker在指定时间内没有收到心跳请求，broker端将会将该消费者移出，并触发重平衡
     */
    public static final String SESSION_TIMEOUT_MS_CONFIG = "session.timeout.ms";
    private static final String SESSION_TIMEOUT_MS_DOC = "The timeout used to detect failures when using Kafka's " +
            "group management facilities. When a consumer's heartbeat is not received within the session timeout, " +
            "the broker will mark the consumer as failed and rebalance the group. Since heartbeats are sent only " +
            "when poll() is invoked, a higher session timeout allows more time for message processing in the consumer's " +
            "poll loop at the cost of a longer time to detect hard failures. See also <code>" + MAX_POLL_RECORDS_CONFIG + "</code> for " +
            "another option to control the processing time in the poll loop. Note that the value must be in the " +
            "allowable range as configured in the broker configuration by <code>group.min.session.timeout.ms</code> " +
            "and <code>group.max.session.timeout.ms</code>.";

    /**
     * <code>heartbeat.interval.ms</code> 心跳间隔时间，消费者会以该频率向broker发送心跳，默认为3s，主要是确保session不会失效
     */
    public static final String HEARTBEAT_INTERVAL_MS_CONFIG = "heartbeat.interval.ms";
    private static final String HEARTBEAT_INTERVAL_MS_DOC = "The expected time between heartbeats to the consumer coordinator when using Kafka's group management facilities. Heartbeats are used to ensure that the consumer's session stays active and to facilitate rebalancing when new consumers join or leave the group. The value must be set lower than <code>session.timeout.ms</code>, but typically should be set no higher than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.";

    /**
     * <code>bootstrap.servers</code> broker服务端地址列表
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /**
     * <code>enable.auto.commit</code> 是否开启自动位点提交，默认为true
     */
    public static final String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";
    private static final String ENABLE_AUTO_COMMIT_DOC = "If true the consumer's offset will be periodically committed in the background.";

    /**
     * <code>auto.commit.interval.ms</code> 如果开启自动位点提交，位点的提交频率，默认为5s
     */
    public static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = "auto.commit.interval.ms";
    private static final String AUTO_COMMIT_INTERVAL_MS_DOC = "The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if <code>enable.auto.commit</code> is set to <code>true</code>.";

    /**
     * <code>partition.assignment.strategy</code> 消费端队列负载算法，默认为按区间平均分配(RangeAssignor),可选值:轮询(RoundRobinAssignor)
     */
    public static final String PARTITION_ASSIGNMENT_STRATEGY_CONFIG = "partition.assignment.strategy";
    private static final String PARTITION_ASSIGNMENT_STRATEGY_DOC = "The class name of the partition assignment strategy that the client will use to distribute partition ownership amongst consumer instances when group management is used";

    /**
     * <code>auto.offset.reset</code> 重置位点策略，但kafka提交位点时，对应的消息已被删除时采取的恢复策略，默认为latest，可选：earliest、none(会抛出异常)
     */
    public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";
    public static final String AUTO_OFFSET_RESET_DOC = "What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted): <ul><li>earliest: automatically reset the offset to the earliest offset<li>latest: automatically reset the offset to the latest offset</li><li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li><li>anything else: throw exception to the consumer.</li></ul>";

    /**
     * <code>fetch.min.bytes</code> 一次拉取消息最小返回的字节数量，默认为1字节
     */
    public static final String FETCH_MIN_BYTES_CONFIG = "fetch.min.bytes";
    private static final String FETCH_MIN_BYTES_DOC = "The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait for that much data to accumulate before answering the request. The default setting of 1 byte means that fetch requests are answered as soon as a single byte of data is available or the fetch request times out waiting for data to arrive. Setting this to something greater than 1 will cause the server to wait for larger amounts of data to accumulate which can improve server throughput a bit at the cost of some additional latency.";

    /**
     * <code>fetch.max.wait.ms</code> fetch等待拉取数据符合fetch.min.bytes的最大等待时间
     */
    public static final String FETCH_MAX_WAIT_MS_CONFIG = "fetch.max.wait.ms";
    private static final String FETCH_MAX_WAIT_MS_DOC = "The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy the requirement given by fetch.min.bytes.";

    /** <code>metadata.max.age.ms</code> 元数据在客户端的过期时间，过期后客户端会向broker重新拉取最新的元数据，默认为5分钟*/
    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;

    /**
     * <code>max.partition.fetch.bytes</code>
     */
    public static final String MAX_PARTITION_FETCH_BYTES_CONFIG = "max.partition.fetch.bytes";
    private static final String MAX_PARTITION_FETCH_BYTES_DOC = "The maximum amount of data per-partition the server will return. The maximum total memory used for a request will be <code>#partitions * max.partition.fetch.bytes</code>. This size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch. If that happens, the consumer can get stuck trying to fetch a large message on a certain partition.";
    public static final int DEFAULT_MAX_PARTITION_FETCH_BYTES = 1 * 1024 * 1024;

    /** <code>send.buffer.bytes</code> 网络通道(TCP)的发送缓存区大小，默认为128K*/
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

    /** <code>receive.buffer.bytes</code> 网络通道(TCP)的接收缓存区大小，默认为32K*/
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

    /**
     * <code>client.id</code> 客户端标识id，默认为consumer-序号，在实践中建议包含客户端IP，在一个消费组中不能重复
     */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

    /**
     * <code>reconnect.backoff.ms</code> 重新建立链接的等待时长，默认为50ms，属于底层网络参数，基本无需关注
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /**
     * <code>retry.backoff.ms</code> 重试间隔时间，默认为100ms
     */
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;

    /**
     * <code>metrics.sample.window.ms</code>
     */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /**
     * <code>metrics.num.samples</code>
     */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

    /**
     * <code>metric.reporters</code>
     */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /**
     * <code>check.crcs</code> 在消费端时是否需要校验CRC，默认为true
     */
    public static final String CHECK_CRCS_CONFIG = "check.crcs";
    private static final String CHECK_CRCS_DOC = "Automatically check the CRC32 of the records consumed. This ensures no on-the-wire or on-disk corruption to the messages occurred. This check adds some overhead, so it may be disabled in cases seeking extreme performance.";

    /** <code>key.deserializer</code> 使用的key序列化类*/
    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";
    public static final String KEY_DESERIALIZER_CLASS_DOC = "Deserializer class for key that implements the <code>Deserializer</code> interface.";

    /** <code>value.deserializer</code> 消息体序列化类*/
    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";
    public static final String VALUE_DESERIALIZER_CLASS_DOC = "Deserializer class for value that implements the <code>Deserializer</code> interface.";

    /** <code>connections.max.idle.ms</code> 连接的最大空闲时间，默认为9s*/
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /** <code>request.timeout.ms</code> 请求的超时时间，与Broker端的网络通讯的请求超时时间*/
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;

    /** <code>interceptor.classes</code> 消费端拦截器，可以有多个*/
    public static final String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
    public static final String INTERCEPTOR_CLASSES_DOC = "A list of classes to use as interceptors. "
                                                        + "Implementing the <code>ConsumerInterceptor</code> interface allows you to intercept (and possibly mutate) records "
                                                        + "received by the consumer. By default, there are no interceptors.";


    /** <code>exclude.internal.topics</code> */
    public static final String EXCLUDE_INTERNAL_TOPICS_CONFIG = "exclude.internal.topics";
    private static final String EXCLUDE_INTERNAL_TOPICS_DOC = "Whether records from internal topics (such as offsets) should be exposed to the consumer. "
                                                            + "If set to <code>true</code> the only way to receive records from an internal topic is subscribing to it.";
    public static final boolean DEFAULT_EXCLUDE_INTERNAL_TOPICS = true;
    
    static {
        CONFIG = new ConfigDef().define(BOOTSTRAP_SERVERS_CONFIG,
                                        Type.LIST,
                                        Importance.HIGH,
                                        CommonClientConfigs.BOOSTRAP_SERVERS_DOC)
                                .define(GROUP_ID_CONFIG, Type.STRING, "", Importance.HIGH, GROUP_ID_DOC)
                                .define(SESSION_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        30000,
                                        Importance.HIGH,
                                        SESSION_TIMEOUT_MS_DOC)
                                .define(HEARTBEAT_INTERVAL_MS_CONFIG,
                                        Type.INT,
                                        3000,
                                        Importance.HIGH,
                                        HEARTBEAT_INTERVAL_MS_DOC)
                                .define(PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                                        Type.LIST,
                                        RangeAssignor.class.getName(),
                                        Importance.MEDIUM,
                                        PARTITION_ASSIGNMENT_STRATEGY_DOC)
                                .define(METADATA_MAX_AGE_CONFIG,
                                        Type.LONG,
                                        5 * 60 * 1000,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METADATA_MAX_AGE_DOC)
                                .define(ENABLE_AUTO_COMMIT_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.MEDIUM,
                                        ENABLE_AUTO_COMMIT_DOC)
                                .define(AUTO_COMMIT_INTERVAL_MS_CONFIG,
                                        Type.LONG,
                                        5000,
                                        atLeast(0),
                                        Importance.LOW,
                                        AUTO_COMMIT_INTERVAL_MS_DOC)
                                .define(CLIENT_ID_CONFIG,
                                        Type.STRING,
                                        "",
                                        Importance.LOW,
                                        CommonClientConfigs.CLIENT_ID_DOC)
                                .define(MAX_PARTITION_FETCH_BYTES_CONFIG,
                                        Type.INT,
                                        DEFAULT_MAX_PARTITION_FETCH_BYTES,
                                        atLeast(0),
                                        Importance.HIGH,
                                        MAX_PARTITION_FETCH_BYTES_DOC)
                                .define(SEND_BUFFER_CONFIG,
                                        Type.INT,
                                        128 * 1024,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SEND_BUFFER_DOC)
                                .define(RECEIVE_BUFFER_CONFIG,
                                        Type.INT,
                                        64 * 1024,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.RECEIVE_BUFFER_DOC)
                                .define(FETCH_MIN_BYTES_CONFIG,
                                        Type.INT,
                                        1,
                                        atLeast(0),
                                        Importance.HIGH,
                                        FETCH_MIN_BYTES_DOC)
                                .define(FETCH_MAX_WAIT_MS_CONFIG,
                                        Type.INT,
                                        500,
                                        atLeast(0),
                                        Importance.LOW,
                                        FETCH_MAX_WAIT_MS_DOC)
                                .define(RECONNECT_BACKOFF_MS_CONFIG,
                                        Type.LONG,
                                        50L,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                                .define(RETRY_BACKOFF_MS_CONFIG,
                                        Type.LONG,
                                        100L,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                                .define(AUTO_OFFSET_RESET_CONFIG,
                                        Type.STRING,
                                        "latest",
                                        in("latest", "earliest", "none"),
                                        Importance.MEDIUM,
                                        AUTO_OFFSET_RESET_DOC)
                                .define(CHECK_CRCS_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.LOW,
                                        CHECK_CRCS_DOC)
                                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                                        Type.LONG,
                                        30000,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                                .define(METRICS_NUM_SAMPLES_CONFIG,
                                        Type.INT,
                                        2,
                                        atLeast(1),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                                .define(METRIC_REPORTER_CLASSES_CONFIG,
                                        Type.LIST,
                                        "",
                                        Importance.LOW,
                                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                                .define(KEY_DESERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        KEY_DESERIALIZER_CLASS_DOC)
                                .define(VALUE_DESERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        VALUE_DESERIALIZER_CLASS_DOC)
                                .define(REQUEST_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        40 * 1000,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        REQUEST_TIMEOUT_MS_DOC)
                                /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
                                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                                        Type.LONG,
                                        9 * 60 * 1000,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                                .define(INTERCEPTOR_CLASSES_CONFIG,
                                        Type.LIST,
                                        null,
                                        Importance.LOW,
                                        INTERCEPTOR_CLASSES_DOC)
                                .define(MAX_POLL_RECORDS_CONFIG,
                                        Type.INT,
                                        Integer.MAX_VALUE,
                                        atLeast(1),
                                        Importance.MEDIUM,
                                        MAX_POLL_RECORDS_DOC)
                                .define(EXCLUDE_INTERNAL_TOPICS_CONFIG,
                                        Type.BOOLEAN,
                                        DEFAULT_EXCLUDE_INTERNAL_TOPICS,
                                        Importance.MEDIUM,
                                        EXCLUDE_INTERNAL_TOPICS_DOC)

                                // security support
                                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                        Type.STRING,
                                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                                .withClientSslSupport()
                                .withClientSaslSupport();

    }

    public static Map<String, Object> addDeserializerToConfig(Map<String, Object> configs,
                                                              Deserializer<?> keyDeserializer,
                                                              Deserializer<?> valueDeserializer) {
        Map<String, Object> newConfigs = new HashMap<String, Object>();
        newConfigs.putAll(configs);
        if (keyDeserializer != null)
            newConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        if (valueDeserializer != null)
            newConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        return newConfigs;
    }

    public static Properties addDeserializerToConfig(Properties properties,
                                                     Deserializer<?> keyDeserializer,
                                                     Deserializer<?> valueDeserializer) {
        Properties newProperties = new Properties();
        newProperties.putAll(properties);
        if (keyDeserializer != null)
            newProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
        if (valueDeserializer != null)
            newProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
        return newProperties;
    }

    ConsumerConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtmlTable());
    }

}
