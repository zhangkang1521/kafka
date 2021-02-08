package org.zhangkang.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerTest {

	private KafkaConsumer consumer;

	private static Logger logger = LoggerFactory.getLogger(ProducerTest.class);

	@Before
	public void before() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1"); // 一个消息只会通知组内的1个消费者
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.zhangkang.test.core.UserDeserializer");
		//		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 默认自动提交
		consumer = new KafkaConsumer<>(props);
	}

	@Test
	public void receive() {
		consumer.subscribe(Arrays.asList("test", "demo"));
		while(true) {
			ConsumerRecords<String, User> records = consumer.poll(1000);
			for (ConsumerRecord<String, User> record : records) {
				logger.info("收到消息：{}", record);
			}
		}
	}
}
