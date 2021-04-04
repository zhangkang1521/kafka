package org.zhangkang.test;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class AdminClientTest {

	AdminClient adminClient;

	@Before
	public void before() {
		Properties properties = new Properties();
		properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		adminClient = AdminClient.create(properties);
	}

	@Test
	public void createTopic() throws Exception {
		NewTopic newTopic = new NewTopic("topic-f", 2, (short)1);
		CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic));
		createTopicsResult.all().get();
	}

	@Test
	public void getConsumeOffset() throws Exception {
		ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets("group-1");
		Map map = result.partitionsToOffsetAndMetadata().get();
		System.out.println(map);
	}

	@Test
	public void listAllTopic() throws Exception {
		ListTopicsResult result = adminClient.listTopics();
		System.out.println(result.names().get());
	}

	@Test
	public void describeTopic() throws Exception {
		DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList("__consumer_offsets"));
		System.out.println(result.all().get());
	}

	@Test
	public void topicConfig() throws Exception {
		DescribeConfigsResult result = adminClient.describeConfigs(Arrays.asList(new ConfigResource(ConfigResource.Type.TOPIC, "topic-a")));
		result.all().get().forEach((configResource, config) -> {
			for (ConfigEntry configEntry : config.entries()) {
				System.out.println(configEntry);
			}
		});
	}
}
