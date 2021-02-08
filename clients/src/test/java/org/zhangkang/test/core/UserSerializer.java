package org.zhangkang.test.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.zhangkang.test.User;

import java.util.Map;

public class UserSerializer implements Serializer<Object> {

	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public byte[] serialize(String topic, Object data) {
		try {
			return objectMapper.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {

	}
}
