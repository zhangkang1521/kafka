package org.zhangkang.test;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ConsumerTest {

	private KafkaConsumer consumer;

	private static Logger logger = LoggerFactory.getLogger(ProducerTest.class);

	@Before
	public void before() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1"); // 一个消息只会通知组内的1个消费者
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		// 自定义序列化
//		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.zhangkang.test.core.UserDeserializer");
		// 默认自动提交
		// props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		// 没有偏移量从哪里开始读取，有偏移量均从已有记录的位置开始读取；默认：latest，
		// earliest:从头开始消费
		// latest: 消费新产生的该分区下的数据
		// none: 只要有一个分区不存在已提交的offset，则抛出异常
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumer = new KafkaConsumer<>(props);
	}

	@Test
	public void receive() {
		consumer.subscribe(Arrays.asList("test"));
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				logger.info("收到消息：{}", record);
			}
		}
	}

	@Test
	public void manualCommitOffset() {
		consumer.subscribe(Arrays.asList("topic-a", "topic-b"));
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (TopicPartition topicPartition : records.partitions()) {
				// 该分区下的消息
				List<ConsumerRecord<String, String>> subRecords = records.records(topicPartition);
				for (ConsumerRecord<String, String> record : subRecords) {
					logger.info("收到消息：{}", record);
				}
				// 手动提交
				// consumer.commitSync();
				// 指定topicPartition提交
				ConsumerRecord<String, String> lastRecord = subRecords.get(subRecords.size() - 1);
				long lastOffset = lastRecord.offset() + 1;
				consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastOffset)));
				logger.info("手动提交：topicPartion:{}, offset:{}", topicPartition, lastOffset);
			}
		}
	}
}
