package org.zhangkang.test.thread;

public class ConsumerMain {
	public static void main(String[] args) {
		new ConsumerGroup(2).execute();
	}
}
