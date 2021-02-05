package org.zhangkang.test;

import org.apache.kafka.clients.producer.*;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerTest {

	Producer<String, String> kafkaProducer;

	private static Logger logger = LoggerFactory.getLogger(ProducerTest.class);

	@Before
	public void before() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// key序列化（必须）
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		// value序列化（必须）
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		// 0：无需确认 1：leader确认收到 all:所有节点确认收到 默认1,默认值在ProducerConfig静态代码块中
//		props.put(ProducerConfig.ACKS_CONFIG, "1");
		kafkaProducer = new KafkaProducer<>(props);
	}

	@Test
	public void testSend() {
		// 往 RecordAccumulator 放，单独的线程 Sender 读取并发送
		kafkaProducer.send(new ProducerRecord<>("test", "hello,world"), (RecordMetadata metadata, Exception exception) -> {
			if (exception == null) {
				logger.info("发送成功, {}", metadata);
			} else {
				logger.error("发送失败", exception);
			}
		});
		kafkaProducer.close();
	}


}
