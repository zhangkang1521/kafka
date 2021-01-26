package org.zhangkang.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.Properties;

public class ProducerTest {

	@Test
	public void testSend() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// key序列化（必须）
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		// value序列化（必须）
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		// 0：无需确认 1：leader确认收到 all:所有节点确认收到 默认1,默认值在ProducerConfig静态代码块中
//		props.put(ProducerConfig.ACKS_CONFIG, "1");

		Producer<String, String> kafkaProducer = new KafkaProducer<>(props);

		// 往 RecordAccumulator 放，单独的线程 Sender 读取并发送
		kafkaProducer.send(new ProducerRecord<>("test", "hello,world"));

		kafkaProducer.close();
	}


}
