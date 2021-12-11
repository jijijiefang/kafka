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

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.LogEntry;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * This class manage the fetching process with the brokers.
 * 消息拉取处理类
 */
public class Fetcher<K, V> {

    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);
    //消费端网络客户端，Kafka 负责网络通讯实现类
    private final ConsumerNetworkClient client;
    private final Time time;
    //一次消息拉取需要拉取的最小字节数，默认值为1字节，如果增大这个值会增大吞吐，但会增加延迟，可以通参数 fetch.min.bytes 改变其默认值
    private final int minBytes;
    //一次消息拉取允许拉取的最大字节数，但这不是绝对的，如果一个分区的第一批记录超过了该值，也会返回。
    //默认为50M,可通过参数 fetch.max.bytes 改变其默认值。同时不能超过 broker的配置参数(message.max.bytes) 和 主题级别的配置(max.message.bytes)
    private final int maxWaitMs;
    //每一个分区返回的最大消息字节数，如果分区中的第一批消息大于 fetchSize 也会返回
    private final int fetchSize;
    //失败重试后需要阻塞的时间，默认为 100 ms，可通过参数 retry.backoff.ms 定制
    private final long retryBackoffMs;
    //单次拉取返回的最大记录数，默认值 500，可通过参数 max.poll.records 进行定制
    private final int maxPollRecords;
    //是否检查消息的 crcs 校验和，默认为 true，可通过参数 check.crcs 进行定制
    private final boolean checkCrcs;
    //元数据
    private final Metadata metadata;
    //消息拉取的统计服务类
    private final FetchManagerMetrics sensors;
    //订阅信息状态
    private final SubscriptionState subscriptions;
    //已经完成的拉取,已完成的 Fetch 的请求结果，待消费端从中取出数据
    private final List<CompletedFetch> completedFetches;
    //key 的反序列化器
    private final Deserializer<K> keyDeserializer;
    //value 的反序列化器
    private final Deserializer<V> valueDeserializer;

    //循环遍历处理已完成主题分区数据中间变量
    private PartitionRecords<K, V> nextInLineRecords = null;

    public Fetcher(ConsumerNetworkClient client,
                   int minBytes,
                   int maxWaitMs,
                   int fetchSize,
                   int maxPollRecords,
                   boolean checkCrcs,
                   Deserializer<K> keyDeserializer,
                   Deserializer<V> valueDeserializer,
                   Metadata metadata,
                   SubscriptionState subscriptions,
                   Metrics metrics,
                   String metricGrpPrefix,
                   Time time,
                   long retryBackoffMs) {
        this.time = time;
        this.client = client;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.minBytes = minBytes;
        this.maxWaitMs = maxWaitMs;
        this.fetchSize = fetchSize;
        this.maxPollRecords = maxPollRecords;
        this.checkCrcs = checkCrcs;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.completedFetches = new ArrayList<>();
        this.sensors = new FetchManagerMetrics(metrics, metricGrpPrefix);
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't already have
     * an in-flight fetch or pending fetch data.
     * 发送拉取消息请求
     */
    public void sendFetches() {
        for (Map.Entry<Node, FetchRequest> fetchEntry: createFetchRequests().entrySet()) {
            final FetchRequest request = fetchEntry.getValue();
            //拉取请求放入待发送集合设置回调
            client.send(fetchEntry.getKey(), ApiKeys.FETCH, request)
                    .addListener(new RequestFutureListener<ClientResponse>() {
                        //拉取成功
                        @Override
                        public void onSuccess(ClientResponse resp) {
                            FetchResponse response = new FetchResponse(resp.responseBody());
                            Set<TopicPartition> partitions = new HashSet<>(response.responseData().keySet());
                            FetchResponseMetricAggregator metricAggregator = new FetchResponseMetricAggregator(sensors, partitions);

                            for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                                TopicPartition partition = entry.getKey();
                                //拉取请求起始偏移量
                                long fetchOffset = request.fetchData().get(partition).offset;
                                FetchResponse.PartitionData fetchData = entry.getValue();
                                //设置完成拉取
                                completedFetches.add(new CompletedFetch(partition, fetchOffset, fetchData, metricAggregator));
                            }

                            sensors.fetchLatency.record(resp.requestLatencyMs());
                            sensors.fetchThrottleTimeSensor.record(response.getThrottleTime());
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            log.debug("Fetch failed", e);
                        }
                    });
        }
    }

    /**
     * 如果需要重置主题分区的偏移量
     * Lookup and set offsets for any partitions which are awaiting an explicit reset.
     * @param partitions the partitions to reset
     */
    public void resetOffsetsIfNeeded(Set<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            // TODO: If there are several offsets to reset, we could submit offset requests in parallel
            if (subscriptions.isAssigned(tp) && subscriptions.isOffsetResetNeeded(tp))
                //使用偏移重置策略重置给定分区的偏移
                resetOffset(tp);
        }
    }

    /**
     * 更新提供的分区的获取位置
     * Update the fetch positions for the provided partitions.
     * @param partitions the partitions to update positions for
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no reset policy is available
     */
    public void updateFetchPositions(Set<TopicPartition> partitions) {
        // reset the fetch position to the committed position
        for (TopicPartition tp : partitions) {
            if (!subscriptions.isAssigned(tp) || subscriptions.isFetchable(tp))
                continue;
            //是否需要重置偏移
            if (subscriptions.isOffsetResetNeeded(tp)) {
                //使用偏移重置策略重置给定分区的偏移
                resetOffset(tp);
            } else if (subscriptions.committed(tp) == null) {
                // there's no committed position, so we need to reset with the default strategy
                subscriptions.needOffsetReset(tp);
                resetOffset(tp);
            } else {
                long committed = subscriptions.committed(tp).offset();
                log.debug("Resetting offset for partition {} to the committed offset {}", tp, committed);
                subscriptions.seek(tp, committed);
            }
        }
    }

    /**
     * Get topic metadata for all topics in the cluster
     * @param timeout time for which getting topic metadata is attempted
     * @return The map of topics with their partition information
     */
    public Map<String, List<PartitionInfo>> getAllTopicMetadata(long timeout) {
        return getTopicMetadata(MetadataRequest.allTopics(), timeout);
    }

    /**
     * Get metadata for all topics present in Kafka cluster
     *
     * @param request The MetadataRequest to send
     * @param timeout time for which getting topic metadata is attempted
     * @return The map of topics with their partition information
     */
    public Map<String, List<PartitionInfo>> getTopicMetadata(MetadataRequest request, long timeout) {
        // Save the round trip if no topics are requested.
        if (!request.isAllTopics() && request.topics().isEmpty())
            return Collections.emptyMap();

        long start = time.milliseconds();
        long remaining = timeout;

        do {
            RequestFuture<ClientResponse> future = sendMetadataRequest(request);
            client.poll(future, remaining);

            if (future.failed() && !future.isRetriable())
                throw future.exception();

            if (future.succeeded()) {
                MetadataResponse response = new MetadataResponse(future.value().responseBody());
                Cluster cluster = response.cluster();

                Set<String> unauthorizedTopics = cluster.unauthorizedTopics();
                if (!unauthorizedTopics.isEmpty())
                    throw new TopicAuthorizationException(unauthorizedTopics);

                boolean shouldRetry = false;
                Map<String, Errors> errors = response.errors();
                if (!errors.isEmpty()) {
                    // if there were errors, we need to check whether they were fatal or whether
                    // we should just retry

                    log.debug("Topic metadata fetch included errors: {}", errors);

                    for (Map.Entry<String, Errors> errorEntry : errors.entrySet()) {
                        String topic = errorEntry.getKey();
                        Errors error = errorEntry.getValue();

                        if (error == Errors.INVALID_TOPIC_EXCEPTION)
                            throw new InvalidTopicException("Topic '" + topic + "' is invalid");
                        else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                            // if a requested topic is unknown, we just continue and let it be absent
                            // in the returned map
                            continue;
                        else if (error.exception() instanceof RetriableException)
                            shouldRetry = true;
                        else
                            throw new KafkaException("Unexpected error fetching metadata for topic " + topic,
                                    error.exception());
                    }
                }

                if (!shouldRetry) {
                    HashMap<String, List<PartitionInfo>> topicsPartitionInfos = new HashMap<>();
                    for (String topic : cluster.topics())
                        topicsPartitionInfos.put(topic, cluster.availablePartitionsForTopic(topic));
                    return topicsPartitionInfos;
                }
            }

            long elapsed = time.milliseconds() - start;
            remaining = timeout - elapsed;

            if (remaining > 0) {
                long backoff = Math.min(remaining, retryBackoffMs);
                time.sleep(backoff);
                remaining -= backoff;
            }
        } while (remaining > 0);

        throw new TimeoutException("Timeout expired while fetching topic metadata");
    }

    /**
     * Send Metadata Request to least loaded node in Kafka cluster asynchronously
     * @return A future that indicates result of sent metadata request
     */
    private RequestFuture<ClientResponse> sendMetadataRequest(MetadataRequest request) {
        final Node node = client.leastLoadedNode();
        if (node == null)
            return RequestFuture.noBrokersAvailable();
        else
            return client.send(node, ApiKeys.METADATA, request);
    }

    /**
     * Reset offsets for the given partition using the offset reset strategy.
     * 使用偏移重置策略重置给定分区的偏移
     * @param partition The given partition that needs reset offset
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException If no offset reset strategy is defined
     */
    private void resetOffset(TopicPartition partition) {
        //重置策略
        OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
        final long timestamp;
        if (strategy == OffsetResetStrategy.EARLIEST)
            timestamp = ListOffsetRequest.EARLIEST_TIMESTAMP;
        else if (strategy == OffsetResetStrategy.LATEST)
            timestamp = ListOffsetRequest.LATEST_TIMESTAMP;
        else
            throw new NoOffsetForPartitionException(partition);

        log.debug("Resetting offset for partition {} to {} offset.", partition, strategy.name().toLowerCase(Locale.ROOT));
        //获取主题分区偏移量
        long offset = listOffset(partition, timestamp);

        // we might lose the assignment while fetching the offset, so check it is still active 我们可能会在获取偏移量时丢失分配，因此请检查它是否仍然处于活动状态
        if (subscriptions.isAssigned(partition))
            this.subscriptions.seek(partition, offset);
    }

    /**
     * Fetch a single offset before the given timestamp for the partition.
     * 在分区的给定时间戳之前获取单个偏移量
     * @param partition The partition that needs fetching offset.
     * @param timestamp The timestamp for fetching offset.
     * @return The offset of the message that is published before the given timestamp
     */
    private long listOffset(TopicPartition partition, long timestamp) {
        while (true) {
            //获取指定主题分区的偏移量
            RequestFuture<Long> future = sendListOffsetRequest(partition, timestamp);
            client.poll(future);

            if (future.succeeded())
                return future.value();

            if (!future.isRetriable())
                throw future.exception();

            if (future.exception() instanceof InvalidMetadataException)
                client.awaitMetadataUpdate();
            else
                time.sleep(retryBackoffMs);
        }
    }

    /**
     * Return the fetched records, empty the record buffer and update the consumed position.
     * 返回获取的记录，清空记录缓冲区并更新消耗的位置。
     * NOTE: returning empty records guarantees the consumed position are NOT updated.
     * 注意：返回空记录保证消耗的位置不会更新。
     * @return The fetched records per partition
     * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
     *         the defaultResetPolicy is NONE
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
        //是否需要向协调器请求分区分配
        if (this.subscriptions.partitionAssignmentNeeded()) {
            return Collections.emptyMap();
        } else {
            Map<TopicPartition, List<ConsumerRecord<K, V>>> drained = new HashMap<>();
            //
            int recordsRemaining = maxPollRecords;
            Iterator<CompletedFetch> completedFetchesIterator = completedFetches.iterator();

            while (recordsRemaining > 0) {
                //解析拉取数据遍历中间变量
                if (nextInLineRecords == null || nextInLineRecords.isEmpty()) {
                    if (!completedFetchesIterator.hasNext())
                        break;
                    //遍历
                    CompletedFetch completion = completedFetchesIterator.next();
                    completedFetchesIterator.remove();
                    //处理拉取的数据赋值给中间变量
                    nextInLineRecords = parseFetchedData(completion);
                } else {
                    //每个主题分区数据放入drained
                    recordsRemaining -= append(drained, nextInLineRecords, recordsRemaining);
                }
            }

            return drained;
        }
    }

    /**
     * 处理主题分区的拉取记录放入drained
     * @param drained
     * @param partitionRecords 主题分区记录
     * @param maxRecords 剩余拉取总记录数
     * @return
     */
    private int append(Map<TopicPartition, List<ConsumerRecord<K, V>>> drained,
                       PartitionRecords<K, V> partitionRecords,
                       int maxRecords) {
        if (partitionRecords.isEmpty())
            return 0;
        //不包含当前主题分区
        if (!subscriptions.isAssigned(partitionRecords.partition)) {
            // this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
            log.debug("Not returning fetched records for partition {} since it is no longer assigned", partitionRecords.partition);
        } else {
            // note that the consumed position should always be available as long as the partition is still assigned
            long position = subscriptions.position(partitionRecords.partition);
            if (!subscriptions.isFetchable(partitionRecords.partition)) {
                // this can happen when a partition is paused before fetched records are returned to the consumer's poll call
                log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable", partitionRecords.partition);
            } else if (partitionRecords.fetchOffset == position) {
                // we are ensured to have at least one record since we already checked for emptiness
                List<ConsumerRecord<K, V>> partRecords = partitionRecords.take(maxRecords);
                //最后一条消息的偏移量+1
                long nextOffset = partRecords.get(partRecords.size() - 1).offset() + 1;

                log.trace("Returning fetched records at offset {} for assigned partition {} and update " +
                        "position to {}", position, partitionRecords.partition, nextOffset);

                List<ConsumerRecord<K, V>> records = drained.get(partitionRecords.partition);
                if (records == null) {
                    records = partRecords;
                    drained.put(partitionRecords.partition, records);
                } else {
                    records.addAll(partRecords);
                }
                //设置此主题分区的拉取位置为nextOffset
                subscriptions.position(partitionRecords.partition, nextOffset);
                return partRecords.size();
            } else {
                // these records aren't next in line based on the last consumed position, ignore them
                // they must be from an obsolete request
                log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
                        partitionRecords.partition, partitionRecords.fetchOffset, position);
            }
        }

        partitionRecords.discard();
        return 0;
    }

    /**
     * Fetch a single offset before the given timestamp for the partition.
     * 在分区的给定时间戳之前获取单个偏移量
     * @param topicPartition The partition that needs fetching offset.
     * @param timestamp The timestamp for fetching offset.
     * @return A response which can be polled to obtain the corresponding offset.
     */
    private RequestFuture<Long> sendListOffsetRequest(final TopicPartition topicPartition, long timestamp) {
        Map<TopicPartition, ListOffsetRequest.PartitionData> partitions = new HashMap<>(1);
        partitions.put(topicPartition, new ListOffsetRequest.PartitionData(timestamp, 1));
        PartitionInfo info = metadata.fetch().partition(topicPartition);
        if (info == null) {
            metadata.add(topicPartition.topic());
            log.debug("Partition {} is unknown for fetching offset, wait for metadata refresh", topicPartition);
            return RequestFuture.staleMetadata();
        } else if (info.leader() == null) {
            log.debug("Leader for partition {} unavailable for fetching offset, wait for metadata refresh", topicPartition);
            return RequestFuture.leaderNotAvailable();
        } else {
            Node node = info.leader();
            ListOffsetRequest request = new ListOffsetRequest(-1, partitions);
            //发送获取偏移量请求
            return client.send(node, ApiKeys.LIST_OFFSETS, request)
                    .compose(new RequestFutureAdapter<ClientResponse, Long>() {
                        @Override
                        public void onSuccess(ClientResponse response, RequestFuture<Long> future) {
                            handleListOffsetResponse(topicPartition, response, future);
                        }
                    });
        }
    }

    /**
     * Callback for the response of the list offset call above. 列表偏移调用响应的回调
     * @param topicPartition The partition that was fetched
     * @param clientResponse The response from the server.
     */
    private void handleListOffsetResponse(TopicPartition topicPartition,
                                          ClientResponse clientResponse,
                                          RequestFuture<Long> future) {
        ListOffsetResponse lor = new ListOffsetResponse(clientResponse.responseBody());
        short errorCode = lor.responseData().get(topicPartition).errorCode;
        if (errorCode == Errors.NONE.code()) {
            List<Long> offsets = lor.responseData().get(topicPartition).offsets;
            if (offsets.size() != 1)
                throw new IllegalStateException("This should not happen.");
            long offset = offsets.get(0);
            log.debug("Fetched offset {} for partition {}", offset, topicPartition);

            future.complete(offset);
        } else if (errorCode == Errors.NOT_LEADER_FOR_PARTITION.code()
                || errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
            log.debug("Attempt to fetch offsets for partition {} failed due to obsolete leadership information, retrying.",
                    topicPartition);
            future.raise(Errors.forCode(errorCode));
        } else {
            log.warn("Attempt to fetch offsets for partition {} failed due to: {}",
                    topicPartition, Errors.forCode(errorCode).message());
            future.raise(new StaleMetadataException());
        }
    }

    /**
     * 可拉取消息的分区
     * @return Set<TopicPartition>
     */
    private Set<TopicPartition> fetchablePartitions() {
        //可以拉取消息的主题分区
        Set<TopicPartition> fetchable = subscriptions.fetchablePartitions();
        if (nextInLineRecords != null && !nextInLineRecords.isEmpty())
            fetchable.remove(nextInLineRecords.partition);
        for (CompletedFetch completedFetch : completedFetches)
            fetchable.remove(completedFetch.partition);
        return fetchable;
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions
     * that have no existing requests in flight.
     * 创建拉取消息请求
     */
    private Map<Node, FetchRequest> createFetchRequests() {
        // create the fetch info
        Cluster cluster = metadata.fetch();
        Map<Node, Map<TopicPartition, FetchRequest.PartitionData>> fetchable = new HashMap<>();
        for (TopicPartition partition : fetchablePartitions()) {
            //获取给定主题分区的当前领导者
            Node node = cluster.leaderFor(partition);
            if (node == null) {
                //如果领导者为空，更新元数据
                metadata.requestUpdate();
            } else if (this.client.pendingRequestCount(node) == 0) {
                // if there is a leader and no in-flight requests, issue a new fetch 如果有领导者并且没有进行中的请求，则发出新的拉取
                Map<TopicPartition, FetchRequest.PartitionData> fetch = fetchable.get(node);
                if (fetch == null) {
                    fetch = new HashMap<>();
                    fetchable.put(node, fetch);
                }
                //最后消费位置
                long position = this.subscriptions.position(partition);
                fetch.put(partition, new FetchRequest.PartitionData(position, this.fetchSize));
                log.trace("Added fetch request for partition {} at offset {}", partition, position);
            }
        }

        // create the fetches 创建拉取请求
        Map<Node, FetchRequest> requests = new HashMap<>();
        for (Map.Entry<Node, Map<TopicPartition, FetchRequest.PartitionData>> entry : fetchable.entrySet()) {
            Node node = entry.getKey();
            FetchRequest fetch = new FetchRequest(this.maxWaitMs, this.minBytes, entry.getValue());
            requests.put(node, fetch);
        }
        return requests;
    }

    /**
     * The callback for fetch completion 获取完成的回调，处理拉取的数据
     */
    private PartitionRecords<K, V> parseFetchedData(CompletedFetch completedFetch) {
        TopicPartition tp = completedFetch.partition;
        FetchResponse.PartitionData partition = completedFetch.partitionData;
        //主题分区拉取的偏移量
        long fetchOffset = completedFetch.fetchedOffset;
        int bytes = 0;
        int recordsCount = 0;
        PartitionRecords<K, V> parsedRecords = null;

        try {
            //判断该分区是否可拉取
            if (!subscriptions.isFetchable(tp)) {
                // this can happen when a rebalance happened or a partition consumption paused
                // while fetch is still in-flight 这可能发生在重新平衡发生或分区消耗暂停而提取仍在进行中时
                log.debug("Ignoring fetched records for partition {} since it is no longer fetchable", tp);
            //正确返回结果
            } else if (partition.errorCode == Errors.NONE.code()) {
                // we are interested in this fetch only if the beginning offset matches the
                // current consumed position 只有当开始偏移量与当前消耗的位置匹配时，我们才对这个获取感兴趣
                //主题分区拉取为位置
                Long position = subscriptions.position(tp);
                //如果当前针对该队列的消费位移与发起 fetch 请求时的 偏移量不一致，则认为本次拉取非法
                if (position == null || position != fetchOffset) {
                    log.debug("Discarding stale fetch response for partition {} since its offset {} does not match " +
                            "the expected offset {}", tp, fetchOffset, position);
                    return null;
                }

                ByteBuffer buffer = partition.recordSet;
                MemoryRecords records = MemoryRecords.readableRecords(buffer);
                List<ConsumerRecord<K, V>> parsed = new ArrayList<>();
                boolean skippedRecords = false;
                for (LogEntry logEntry : records) {
                    // Skip the messages earlier than current position. 跳过比当前位置更早的消息
                    if (logEntry.offset() >= position) {
                        //解析消息条目放入已解析消息条目集合
                        parsed.add(parseRecord(tp, logEntry));
                        bytes += logEntry.size();
                    } else {
                        skippedRecords = true;
                    }
                }

                recordsCount = parsed.size();
                this.sensors.recordTopicFetchMetrics(tp.topic(), bytes, recordsCount);
                //已解析消息条目集合不为空
                if (!parsed.isEmpty()) {
                    log.trace("Adding fetched record for partition {} with offset {} to buffered record list", tp, position);
                    parsedRecords = new PartitionRecords<>(fetchOffset, tp, parsed);
                    ConsumerRecord<K, V> record = parsed.get(parsed.size() - 1);
                    this.sensors.recordsFetchLag.record(partition.highWatermark - record.offset());
                } else if (buffer.limit() > 0 && !skippedRecords) {
                    // we did not read a single message from a non-empty buffer
                    // because that message's size is larger than fetch size, in this case
                    // record this exception
                    Map<TopicPartition, Long> recordTooLargePartitions = Collections.singletonMap(tp, fetchOffset);
                    throw new RecordTooLargeException("There are some messages at [Partition=Offset]: "
                            + recordTooLargePartitions
                            + " whose size is larger than the fetch size "
                            + this.fetchSize
                            + " and hence cannot be ever returned."
                            + " Increase the fetch size on the client (using max.partition.fetch.bytes),"
                            + " or decrease the maximum message size the broker will allow (using message.max.bytes).",
                            recordTooLargePartitions);
                }
            } else if (partition.errorCode == Errors.NOT_LEADER_FOR_PARTITION.code()
                    || partition.errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
                this.metadata.requestUpdate();
            } else if (partition.errorCode == Errors.OFFSET_OUT_OF_RANGE.code()) {
                if (fetchOffset != subscriptions.position(tp)) {
                    log.debug("Discarding stale fetch response for partition {} since the fetched offset {}" +
                            "does not match the current offset {}", tp, fetchOffset, subscriptions.position(tp));
                } else if (subscriptions.hasDefaultOffsetResetPolicy()) {
                    log.info("Fetch offset {} is out of range for partition {}, resetting offset", fetchOffset, tp);
                    subscriptions.needOffsetReset(tp);
                } else {
                    throw new OffsetOutOfRangeException(Collections.singletonMap(tp, fetchOffset));
                }
            } else if (partition.errorCode == Errors.TOPIC_AUTHORIZATION_FAILED.code()) {
                log.warn("Not authorized to read from topic {}.", tp.topic());
                throw new TopicAuthorizationException(Collections.singleton(tp.topic()));
            } else if (partition.errorCode == Errors.UNKNOWN.code()) {
                log.warn("Unknown error fetching data for topic-partition {}", tp);
            } else {
                throw new IllegalStateException("Unexpected error code " + partition.errorCode + " while fetching data");
            }
        } finally {
            completedFetch.metricAggregator.record(tp, bytes, recordsCount);
        }

        return parsedRecords;
    }

    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     * 解析记录条目，必要时反序列化键/值字段
     */
    private ConsumerRecord<K, V> parseRecord(TopicPartition partition, LogEntry logEntry) {
        Record record = logEntry.record();
        //校验CRC
        if (this.checkCrcs && !record.isValid())
            throw new KafkaException("Record for partition " + partition + " at offset "
                    + logEntry.offset() + " is corrupt (stored crc = " + record.checksum()
                    + ", computed crc = "
                    + record.computeChecksum()
                    + ")");

        try {
            long offset = logEntry.offset();
            long timestamp = record.timestamp();
            TimestampType timestampType = record.timestampType();
            ByteBuffer keyBytes = record.key();
            byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
            K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), keyByteArray);
            ByteBuffer valueBytes = record.value();
            byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
            V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(), valueByteArray);

            return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
                                        timestamp, timestampType, record.checksum(),
                                        keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
                                        valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
                                        key, value);
        } catch (RuntimeException e) {
            throw new SerializationException("Error deserializing key/value for partition " + partition +
                    " at offset " + logEntry.offset(), e);
        }
    }

    /**
     * 主题分区消息记录集
     * @param <K>
     * @param <V>
     */
    private static class PartitionRecords<K, V> {
        private long fetchOffset;
        //分区信息
        private TopicPartition partition;
        private List<ConsumerRecord<K, V>> records;

        public PartitionRecords(long fetchOffset, TopicPartition partition, List<ConsumerRecord<K, V>> records) {
            this.fetchOffset = fetchOffset;
            this.partition = partition;
            this.records = records;
        }

        private boolean isEmpty() {
            return records == null || records.isEmpty();
        }

        private void discard() {
            this.records = null;
        }

        private List<ConsumerRecord<K, V>> take(int n) {
            if (records == null)
                return new ArrayList<>();

            if (n >= records.size()) {
                List<ConsumerRecord<K, V>> res = this.records;
                this.records = null;
                return res;
            }

            List<ConsumerRecord<K, V>> res = new ArrayList<>(n);
            Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
            for (int i = 0; i < n; i++) {
                res.add(iterator.next());
                iterator.remove();
            }

            if (iterator.hasNext())
                this.fetchOffset = iterator.next().offset();

            return res;
        }
    }

    /**
     * 完成的拉取
     */
    private static class CompletedFetch {
        //主题分区
        private final TopicPartition partition;
        //本次拉取的开始偏移量
        private final long fetchedOffset;
        //分区数据
        private final FetchResponse.PartitionData partitionData;
        //统计指标相关
        private final FetchResponseMetricAggregator metricAggregator;

        public CompletedFetch(TopicPartition partition,
                              long fetchedOffset,
                              FetchResponse.PartitionData partitionData,
                              FetchResponseMetricAggregator metricAggregator) {
            this.partition = partition;
            this.fetchedOffset = fetchedOffset;
            this.partitionData = partitionData;
            this.metricAggregator = metricAggregator;
        }
    }

    /**
     * Since we parse the message data for each partition from each fetch response lazily, fetch-level
     * metrics need to be aggregated as the messages from each partition are parsed. This class is used
     * to facilitate this incremental aggregation.
     */
    private static class FetchResponseMetricAggregator {
        private final FetchManagerMetrics sensors;
        private final Set<TopicPartition> unrecordedPartitions;

        private int totalBytes;
        private int totalRecords;

        public FetchResponseMetricAggregator(FetchManagerMetrics sensors,
                                             Set<TopicPartition> partitions) {
            this.sensors = sensors;
            this.unrecordedPartitions = partitions;
        }

        /**
         * After each partition is parsed, we update the current metric totals with the total bytes
         * and number of records parsed. After all partitions have reported, we write the metric.
         */
        public void record(TopicPartition partition, int bytes, int records) {
            unrecordedPartitions.remove(partition);
            totalBytes += bytes;
            totalRecords += records;

            if (unrecordedPartitions.isEmpty()) {
                // once all expected partitions from the fetch have reported in, record the metrics
                sensors.bytesFetched.record(totalBytes);
                sensors.recordsFetched.record(totalRecords);
            }
        }
    }

    private static class FetchManagerMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor bytesFetched;
        public final Sensor recordsFetched;
        public final Sensor fetchLatency;
        public final Sensor recordsFetchLag;
        public final Sensor fetchThrottleTimeSensor;

        public FetchManagerMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-fetch-manager-metrics";

            this.bytesFetched = metrics.sensor("bytes-fetched");
            this.bytesFetched.add(metrics.metricName("fetch-size-avg",
                this.metricGrpName,
                "The average number of bytes fetched per request"), new Avg());
            this.bytesFetched.add(metrics.metricName("fetch-size-max",
                this.metricGrpName,
                "The maximum number of bytes fetched per request"), new Max());
            this.bytesFetched.add(metrics.metricName("bytes-consumed-rate",
                this.metricGrpName,
                "The average number of bytes consumed per second"), new Rate());

            this.recordsFetched = metrics.sensor("records-fetched");
            this.recordsFetched.add(metrics.metricName("records-per-request-avg",
                this.metricGrpName,
                "The average number of records in each request"), new Avg());
            this.recordsFetched.add(metrics.metricName("records-consumed-rate",
                this.metricGrpName,
                "The average number of records consumed per second"), new Rate());

            this.fetchLatency = metrics.sensor("fetch-latency");
            this.fetchLatency.add(metrics.metricName("fetch-latency-avg",
                this.metricGrpName,
                "The average time taken for a fetch request."), new Avg());
            this.fetchLatency.add(metrics.metricName("fetch-latency-max",
                this.metricGrpName,
                "The max time taken for any fetch request."), new Max());
            this.fetchLatency.add(metrics.metricName("fetch-rate",
                this.metricGrpName,
                "The number of fetch requests per second."), new Rate(new Count()));

            this.recordsFetchLag = metrics.sensor("records-lag");
            this.recordsFetchLag.add(metrics.metricName("records-lag-max",
                this.metricGrpName,
                "The maximum lag in terms of number of records for any partition in this window"), new Max());

            this.fetchThrottleTimeSensor = metrics.sensor("fetch-throttle-time");
            this.fetchThrottleTimeSensor.add(metrics.metricName("fetch-throttle-time-avg",
                                                         this.metricGrpName,
                                                         "The average throttle time in ms"), new Avg());

            this.fetchThrottleTimeSensor.add(metrics.metricName("fetch-throttle-time-max",
                                                         this.metricGrpName,
                                                         "The maximum throttle time in ms"), new Max());
        }

        public void recordTopicFetchMetrics(String topic, int bytes, int records) {
            // record bytes fetched
            String name = "topic." + topic + ".bytes-fetched";
            Sensor bytesFetched = this.metrics.getSensor(name);
            if (bytesFetched == null) {
                Map<String, String> metricTags = new HashMap<>(1);
                metricTags.put("topic", topic.replace('.', '_'));

                bytesFetched = this.metrics.sensor(name);
                bytesFetched.add(this.metrics.metricName("fetch-size-avg",
                        this.metricGrpName,
                        "The average number of bytes fetched per request for topic " + topic,
                        metricTags), new Avg());
                bytesFetched.add(this.metrics.metricName("fetch-size-max",
                        this.metricGrpName,
                        "The maximum number of bytes fetched per request for topic " + topic,
                        metricTags), new Max());
                bytesFetched.add(this.metrics.metricName("bytes-consumed-rate",
                        this.metricGrpName,
                        "The average number of bytes consumed per second for topic " + topic,
                        metricTags), new Rate());
            }
            bytesFetched.record(bytes);

            // record records fetched
            name = "topic." + topic + ".records-fetched";
            Sensor recordsFetched = this.metrics.getSensor(name);
            if (recordsFetched == null) {
                Map<String, String> metricTags = new HashMap<>(1);
                metricTags.put("topic", topic.replace('.', '_'));

                recordsFetched = this.metrics.sensor(name);
                recordsFetched.add(this.metrics.metricName("records-per-request-avg",
                        this.metricGrpName,
                        "The average number of records in each request for topic " + topic,
                        metricTags), new Avg());
                recordsFetched.add(this.metrics.metricName("records-consumed-rate",
                        this.metricGrpName,
                        "The average number of records consumed per second for topic " + topic,
                        metricTags), new Rate());
            }
            recordsFetched.record(records);
        }
    }
}
