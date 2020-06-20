package org.chenguoyu.kafka.procuder;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Procuder {
    private static final String brokerList = "localhost:9092";
    private static final String topic = "topic-demo";
    private static Callback callback = new Callback() {
        /**
         * metadata与exception是互斥的，metadata为null则exception不为null，反之亦然
         * @param metadata
         * @param exception
         */
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                System.out.println(metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
            }
        }
    };

    public static Properties initConfig() {
        Properties properties = new Properties();
        // key序列化方式
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // value序列化方式
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 如果有多个，则以 , 号分隔
        properties.put("bootstrap.servers", brokerList);
        // 设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        return properties;
    }

    /**
     * 发后即忘，不关心消息是否正确到达
     */
    @Test
    public void fireAndForget() {
        Properties properties = initConfig();
        // 配置生产者客户端，并创建KafkaProducer实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 构建所需要发送的消息
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello kafka!");
        producer.send(record);
        producer.close();
    }

    /**
     * 同步方式
     */
    @Test
    public void sync1() {
        Properties properties = initConfig();
        // 配置生产者客户端，并创建KafkaProducer实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 构建所需要发送的消息
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello kafka!");
        try {
            // send方法本身是异步的，是Future.get()方法阻塞了当前线程
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            System.out.println(metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        producer.close();
    }

    /**
     * 通过回调函数确认
     */
    @Test
    public void sync2() {
        Properties properties = initConfig();
        // 配置生产者客户端，并创建KafkaProducer实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 构建所需要发送的消息
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello kafka!");
        producer.send(record, callback);
        producer.close();
    }

    /**
     * 如果record1和record2发送到同一个分区，那么record的回调函数一定比record2的回调函数先调用
     */
    @Test
    public void sync3() {
        Properties properties = initConfig();
        // 配置生产者客户端，并创建KafkaProducer实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 构建所需要发送的消息
        ProducerRecord<String, String> record1 = new ProducerRecord<>(topic, "hello kafka!");
        producer.send(record1, callback);
        ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, "hello kafka!");
        producer.send(record2, callback);
        producer.close();
    }

    /**
     * 使用自定义的序列化器
     */
    @Test
    public void mySerializer() {
        Properties properties = initConfig();
        // value序列化方式 指定自己创建的序列化类
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MySerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello kafka!");
        producer.send(record, callback);
        producer.close();
    }

    /**
     * 使用自定义的分区器
     */
    @Test
    public void myPartitioner() {
        Properties properties = initConfig();
        // 使用自定义的分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello kafka!");
        producer.send(record, callback);
        producer.close();
    }

    /**
     * 使用自定义的生产者过滤器
     */
    @Test
    public void myProducerInterceptor() {
        Properties properties = initConfig();
        // 使用自定义的分区器
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyProducerInterceptor.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello kafka!");
        producer.send(record, callback);
        producer.close();
    }
}
