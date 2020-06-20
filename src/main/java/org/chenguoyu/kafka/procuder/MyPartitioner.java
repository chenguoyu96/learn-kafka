package org.chenguoyu.kafka.procuder;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MyPartitioner implements Partitioner {
    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numPartition = partitionInfos.size();
        if (keyBytes == null) {
            return counter.getAndIncrement() % numPartition;
        } else {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartition;
        }

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
