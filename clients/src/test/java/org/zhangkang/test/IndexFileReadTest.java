package org.zhangkang.test;

import org.apache.kafka.common.utils.ByteUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class IndexFileReadTest {

	@Test
	public void read() throws Exception {
		// 读取批量消息
		FileInputStream in = new FileInputStream("E:\\github2\\kafka-logs\\topic-i-0\\00000000000000000000.index");
		FileChannel channel = in.getChannel();
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		channel.read(buffer);
		buffer.flip();

		// 批量消息头部
		int offset0 = buffer.getInt(); //
		int position0 = buffer.getInt(); //

		int offset1 = buffer.getInt(); //
		int position1 = buffer.getInt(); //

		int offset2 = buffer.getInt(); //
		int position2 = buffer.getInt(); //



		channel.close();
		in.close();



	}


}
