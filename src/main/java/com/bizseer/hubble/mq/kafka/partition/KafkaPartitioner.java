package com.bizseer.hubble.mq.kafka.partition;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import java.util.Random;

@SuppressWarnings("deprecation")
public class KafkaPartitioner implements Topic {
	
	public KafkaPartitioner() {}

	public KafkaPartitioner(VerifiableProperties props) {
	}

	@Override
	public int partition(Object key, int numPartitions) {
		int partition = 0;
		if (key != null) {
			int i = Math.abs(key.hashCode() % numPartitions);
			partition = i == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(i);
		} else {
			Random random = new Random();
			partition = random.nextInt(numPartitions);
		}
		return partition;
	}

	/*
	public static void main(String[] args) {
		int a = Math.abs(-1232332232) % 3;
		System.out.println(a);

		int b = 1232332232 % 3;
		System.out.println(b);
	}*/

}