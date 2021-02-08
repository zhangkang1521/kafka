package org.zhangkang.test.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.zhangkang.test.User;

import java.io.IOException;
import java.util.Map;

public class UserDeserializer implements Deserializer<User> {

	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public User deserialize(String topic, byte[] data) {

		try {
			return objectMapper.readValue(data, User.class);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {

	}
}
