package org.zhangkang.test.thread;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhangkang.test.ProducerTest;

import java.util.Arrays;
import java.util.Properties;

/**
 * 每个线程独立的KafkaConsumer进行消费
 */
public class ConsumerRunnable implements Runnable {

	private KafkaConsumer consumer;

	private static Logger logger = LoggerFactory.getLogger(ProducerTest.class);

	public ConsumerRunnable() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1"); // 一个消息只会通知组内的1个消费者
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(props);
	}


	@Override
	public void run() {
		consumer.subscribe(Arrays.asList("test"));
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				logger.info("收到消息：{}", record);
			}
		}
	}
}
