package org.zhangkang.test;

import org.apache.kafka.common.utils.ByteUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class LogFileReadTest {

	@Test
	public void read() throws Exception {
		// 读取批量消息
		FileInputStream in = new FileInputStream("E:\\github2\\kafka-logs\\topic-i-0\\00000000000000000000.log");
		FileChannel channel = in.getChannel();
		ByteBuffer buffer = ByteBuffer.allocate(1024*100);
		channel.read(buffer);
		buffer.flip();

		buffer.position(4796);

		// 批量消息头部
		long baseOffset = buffer.getLong(); // 偏移
		int sizeInBytes = buffer.getInt(); // 消息体长度(不包含偏移和消息体长度本身 81-12=69)
		int partitionLeaderEpoch = buffer.getInt(); // 分区领导者纪元
		byte magic = buffer.get(); // 魔术值，版本2
		int crc = buffer.getInt(); // 校验码
		short attributes = buffer.getShort(); // 属性值
		int lastOffsetDelta = buffer.getInt(); // 相对位移差值

		long firstTimestamp = buffer.getLong(); // 第一条消息时间戳
		long maxTimestamp = buffer.getLong(); // 最后1条消息时间戳

		long producerId = buffer.getLong(); // 生产者id
		short epoch = buffer.getShort(); // 生产者纪元

		int sequence = buffer.getInt(); // 序号
		int numRecords = buffer.getInt(); // 记录数量

		readRecord(buffer);
		readRecord(buffer);


		channel.close();
		in.close();



	}

	private void readRecord(ByteBuffer buffer) {
		// 读取消息

		// 消息体长度
		// int sizeInBytes = sizeOfBodyInBytes(offsetDelta, timestampDelta, key, value, headers);
		int bodyLength = ByteUtils.readVarint(buffer);

		// 属性
		byte attribute = buffer.get();

		// 相对时间戳
		long timestampDelta = ByteUtils.readVarlong(buffer);
		// 相对offset
		int offsetDelta = ByteUtils.readVarint(buffer);

		int keyLen = ByteUtils.readVarint(buffer);

		int valueLen = ByteUtils.readVarint(buffer);

		byte[] valueBytes = new byte[valueLen];
		buffer.get(valueBytes);
		System.out.println(new String(valueBytes));


		// 头部
		int headLen = ByteUtils.readVarint(buffer);
	}
}
