package org.zhangkang.test.core;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CounterProducerInterceptor implements ProducerInterceptor {

	private Logger logger = LoggerFactory.getLogger(CounterProducerInterceptor.class);

	@Override
	public ProducerRecord onSend(ProducerRecord record) {
		return record;
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		if (exception == null) {
			logger.info("成功：{}", metadata);
		} else {
			logger.error("失败，{}", metadata, exception);
		}
	}

	@Override
	public void close() {

	}

	@Override
	public void configure(Map<String, ?> configs) {

	}
}
