package org.zhangkang.test.core;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;

import java.util.Map;

public class AppNameProducerInterceptor implements ProducerInterceptor {
	@Override
	public ProducerRecord onSend(ProducerRecord record) {
		ProducerRecord producerRecord = new ProducerRecord(record.topic(), record.partition(), record.timestamp(), record.key(), "app:order" + record.value(), record.headers());
		return producerRecord;
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

	}

	@Override
	public void close() {

	}

	@Override
	public void configure(Map<String, ?> configs) {

	}
}
