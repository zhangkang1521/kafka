package org.zhangkang.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class ObjectMapperTest {

	@Test
	public void testWrite() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		User user = new User();
		user.setId(100);
		user.setUsername("zk");
		byte[] bytes = objectMapper.writeValueAsBytes(user);

		User user2 = objectMapper.readValue(bytes, User.class);
		System.out.println(user2);

	}
}
