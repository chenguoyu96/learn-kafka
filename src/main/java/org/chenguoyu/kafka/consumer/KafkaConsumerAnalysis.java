package org.chenguoyu.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * kafka消费者
 * kafka提供了三种订阅主题的方式，数组，正则和指定分区，不过三种方式是互斥的
 */
public class KafkaConsumerAnalysis {
    private static final String brokerList = "localhost:9092";
    private static final String topic = "topic-demo";
    private static final String topic2 = "topic-demo2";
    private static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties properties = new Properties();
        // key反序列化方式
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // value反序列化方式
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // kafka集群清单，如果有多个，则以 , 号分隔，可以不写全部，kafka会自动发现
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        // 消费者组名称，不能为空，默认为"",一般设置成具有一定的业务意义的名称
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // consumer客户端id
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        // 指定客户端拦截器
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,ConsumerInterceptorTTL.class.getName());
        return properties;
    }

    @Test
    public void subscribeTopic() {
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 通过数组订阅主题
        consumer.subscribe(Arrays.asList(topic));
        // 通过正则表达式订阅主题
        consumer.subscribe(Pattern.compile("topic-.*"));
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    System.out.println("key = " + record.key() + ", value = " + record.value());
                }
            }
        } finally {
            // 未指定关闭时间，默认是30秒
            consumer.close();
        }
    }

    /**
     * 通过正则表达式订阅主题
     */
    @Test
    public void subscribeTopicByRegex() {
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 通过正则表达式订阅主题
        consumer.subscribe(Pattern.compile("topic-.*"));
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    System.out.println("key = " + record.key() + ", value = " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 订阅主题的指定分区
     */
    @Test
    public void subscribeTopicAndPartition() {
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅主题指定分区
        consumer.assign(Arrays.asList(new TopicPartition("topic-demo", 0)));
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    System.out.println("key = " + record.key() + ", value = " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 订阅主题的所有分区
     */
    @Test
    public void subscribeTopicAndAllPartition() {
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Pattern.compile("topic-.*"));
        List<TopicPartition> partitions = new ArrayList<>();
        // 获得所有分区
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos != null) {
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
        }
        consumer.assign(partitions);
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    System.out.println("key = " + record.key() + ", value = " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 以分区为维度消费
     */
    @Test
    public void consumerByPartitions() {
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 通过数组订阅主题
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                Set<TopicPartition> partitions = records.partitions();
                // 暂停消费
                consumer.pause(partitions);
                // 恢复消费
                consumer.resume(partitions);
                for (TopicPartition partition : partitions) {
                    for (ConsumerRecord<String, String> record : records.records(partition)) {
                        System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                        System.out.println("key = " + record.key() + ", value = " + record.value());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 以主题为维度消费
     */
    @Test
    public void consumerByTopic() {
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        List<String> topics = Arrays.asList(topic, topic2);
        consumer.subscribe(topics);
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (String topic : topics) {
                    for (ConsumerRecord<String, String> record : records.records(topic)) {
                        System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                        System.out.println("key = " + record.key() + ", value = " + record.value());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 关闭消费者连接的第二种方式
     */
    @Test
    public void consumerWakeup() {
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        List<String> topics = Arrays.asList(topic, topic2);
        consumer.subscribe(topics);
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    System.out.println("key = " + record.key() + ", value = " + record.value());
                }
                consumer.wakeup();
            }
        } catch (WakeupException e){
            // 不需要处理该异常，他只是跳出循环的一种方式
        } finally{
            consumer.close();
        }
    }

    /**
     * 指定位移消费
     */
    @Test
    public void consumerSeek() {
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        List<String> topics = Arrays.asList(topic, topic2);
        consumer.subscribe(topics);
        // 在调用seek方法之前必须先执行poll方法，获得分区信息
        consumer.poll(Duration.ofMillis(1000));
        Set<TopicPartition> assignment = consumer.assignment();
        // 每个分区都从第10个位移开始消费
        for (TopicPartition topicPartition : assignment) {
            consumer.seek(topicPartition,10);
        }
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    System.out.println("key = " + record.key() + ", value = " + record.value());
                }
            }
        }  finally{
            consumer.close();
        }
    }

    /**
     * 指定从分区的末尾或
     */
    @Test
    public void consumerSeekBeginOrEnd() {
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        List<String> topics = Arrays.asList(topic, topic2);
        consumer.subscribe(topics);
        // 在调用seek方法之前必须先执行poll方法，获得分区信息
        consumer.poll(Duration.ofMillis(1000));
        Set<TopicPartition> assignment = consumer.assignment();
        // 从分区末尾开始消费
//        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        // 从分区开头开始消费
        Map<TopicPartition, Long> offsets = consumer.beginningOffsets(assignment);
        for (TopicPartition topicPartition : assignment) {
            consumer.seek(topicPartition,offsets.get(topicPartition));
        }
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    System.out.println("key = " + record.key() + ", value = " + record.value());
                }
            }
        }  finally{
            consumer.close();
        }
    }

    /**
     * 指定从分区的某个时间点的位移开始消费
     */
    @Test
    public void consumerOffsetForTime() {
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        List<String> topics = Arrays.asList(topic, topic2);
        consumer.subscribe(topics);
        // 在调用seek方法之前必须先执行poll方法，获得分区信息
        consumer.poll(Duration.ofMillis(1000));
        Set<TopicPartition> assignment = consumer.assignment();
        Map<TopicPartition,Long> timestampToSearch = new HashMap<>();
        for (TopicPartition topicPartition : assignment) {
            timestampToSearch.put(topicPartition,System.currentTimeMillis()-24*60*60*1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampToSearch);
        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(topicPartition);
            if (offsetAndTimestamp!=null){
                consumer.seek(topicPartition,offsetAndTimestamp.offset());
            }
        }
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    System.out.println("key = " + record.key() + ", value = " + record.value());
                }
            }
        }  finally{
            consumer.close();
        }
    }
}
