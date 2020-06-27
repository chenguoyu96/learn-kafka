package org.chenguoyu.kafka.manage;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaAdminManage {
    private String brokerList = "localhost:9092";
    private String topic = "topic-admin";
    private AdminClient client;

    /**
     * 通过指定分区数与负载因子创建主题
     */
    @Test
    public void createTopic() {


        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);
        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 自定义分区数与副本分配
     */
    @Test
    public void createTopics1() {
        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        replicasAssignments.put(0, Collections.singletonList(0));
        replicasAssignments.put(1, Collections.singletonList(0));
        replicasAssignments.put(2, Collections.singletonList(0));
        replicasAssignments.put(3, Collections.singletonList(0));
        NewTopic newTopic = new NewTopic(topic, replicasAssignments);
        // 指定需要覆盖的配置
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "compact");
        newTopic.configs(configs);
        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查看主题列表
     */
    @Test
    public void listTopic() {
        ListTopicsResult listTopicsResult = client.listTopics();
        try {
            Collection<TopicListing> topicListings = listTopicsResult.listings().get();
            topicListings.forEach(System.out::println);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查看主题详情
     */
    @Test
    public void describeTopics() {
        ListTopicsResult listTopicsResult = client.listTopics();
        try {
            Collection<TopicListing> topicListings = listTopicsResult.listings().get();
            List<String> topicsName = topicListings.stream().map(TopicListing::name).collect(Collectors.toList());
            client.describeTopics(topicsName);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查看主题配置信息
     */
    @Test
    public void describeTopicsConfig() throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        DescribeConfigsResult result = client.describeConfigs(Collections.singletonList(resource));
        Config config = result.all().get().get(resource);
        System.out.println(config);
    }

    /**
     * 修改主题配置信息
     */
    @Test
    public void alterTopicsConfig() throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        ConfigEntry entry = new ConfigEntry("cleanup.policy", "compact");
        Config config = new Config(Collections.singletonList(entry));
        Map<ConfigResource, Config> configs = new HashMap<>();
        configs.put(resource, config);
        AlterConfigsResult result = client.alterConfigs(configs);
        result.all().get();
    }

    @Before
    public void before() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        client = AdminClient.create(props);
    }

    @After
    public void after() {
        client.close();
    }
}
