package org.zhangkang.test.core;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class DemoPartitioner implements Partitioner {

	private Logger logger = LoggerFactory.getLogger(DemoPartitioner.class);

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		int numPartitions = partitions.size();
		logger.debug("topic:{}, 总分区数：{}", topic, numPartitions);
		if ("audit".equals(key)) { // key 是audit放到最后1个分区
			logger.debug("key是audit，返回最后1个分区");
			return numPartitions - 1;
		} else {
			logger.debug("key不是audit，返回最后1个分区外分区");
			return new Random().nextInt(numPartitions -  1);
		}
	}

	@Override
	public void close() {

	}

	@Override
	public void configure(Map<String, ?> configs) {

	}
}
