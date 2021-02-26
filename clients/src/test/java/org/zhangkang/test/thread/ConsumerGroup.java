package org.zhangkang.test.thread;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup {

	private List<ConsumerRunnable> consumerRunnables = new ArrayList<>();

	public ConsumerGroup(int threadNum) {
		for (int i = 0; i < threadNum; i++) {
			ConsumerRunnable consumerRunnable = new ConsumerRunnable();
			consumerRunnables.add(consumerRunnable);
		}
	}

	public void execute() {
		for (ConsumerRunnable consumerRunnable : consumerRunnables) {
			new Thread(consumerRunnable).start();
		}
	}
}
