package org.zhangkang.test;

import org.junit.Test;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TimeIndexFileReadTest {

	@Test
	public void read() throws Exception {
		FileInputStream in = new FileInputStream("E:\\github2\\kafka-logs\\topic-i-0\\00000000000000000000.timeindex");
		FileChannel channel = in.getChannel();
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		channel.read(buffer);
		buffer.flip();

		long time0 = buffer.getLong();
		int offset0 = buffer.getInt(); // 逻辑位移，物理位置需到Index文件中找

		long time1 = buffer.getLong();
		int offset1 = buffer.getInt();

		long time2 = buffer.getLong();
		int offset2 = buffer.getInt();

		long time3 = buffer.getLong();
		int offset3 = buffer.getInt();

		channel.close();
		in.close();



	}


}
