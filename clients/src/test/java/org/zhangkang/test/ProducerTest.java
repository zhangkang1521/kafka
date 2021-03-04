package org.zhangkang.test;

import org.apache.kafka.clients.producer.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerTest {



	KafkaProducer<String, Object> kafkaProducer;

	private static Logger logger = LoggerFactory.getLogger(ProducerTest.class);

	@Before
	public void before() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// key序列化（必须）
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		// value序列化（必须）
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		// 自定义序列化
//		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.zhangkang.test.core.UserSerializer");
		// 0：无需确认 1：leader确认收到 all:所有节点确认收到 默认1,默认值在ProducerConfig静态代码块中
//		props.put(ProducerConfig.ACKS_CONFIG, "1");
		// 自定义分区策略
//		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.zhangkang.test.core.DemoPartitioner");
		// 拦截器
		// props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.zhangkang.test.core.AppNameProducerInterceptor,org.zhangkang.test.core.CounterProducerInterceptor");
		kafkaProducer = new KafkaProducer<>(props);
	}

	@Test
	public void testSend() {
		// 往 RecordAccumulator 放，单独的线程 Sender 读取并发送
		for (int i = 0; i < 2000; i++) {
			kafkaProducer.send(new ProducerRecord<>("topic-i", "ad" + i), (RecordMetadata metadata, Exception exception) -> {
				if (exception == null) {
					logger.info("发送成功, {}", metadata);
				} else {
					logger.error("发送失败", exception);
				}
			});
		}
	}

	@Test
	public void testSerialize() {
		User user = new User();
		user.setId(100);
		user.setUsername("zk");
		kafkaProducer.send(new ProducerRecord<>("user-topic", user), (RecordMetadata metadata, Exception exception) -> {
			if (exception == null) {
				logger.info("发送成功, {}", metadata);
			} else {
				logger.error("发送失败", exception);
			}
		});

	}

	@Test
	public void partition() {
		// kafka-run-class.bat  kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic test-topic
		for (int i = 0; i < 10; i++) {
			kafkaProducer.send(new ProducerRecord<>("test-topic", "audit", "hello,world"), (RecordMetadata metadata, Exception exception) -> {
				if (exception == null) {
					logger.info("发送成功, {}", metadata);
				} else {
					logger.error("发送失败", exception);
				}
			});
		}
	}

	@After
	public void after() {
		kafkaProducer.close();
	}


}
