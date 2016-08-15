package io.phaas.kafka_group_manager;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;

/**
 * Created by patrick on 8/13/2016.
 */
public class Consumer {

	public static void main(String[] args) {
		Environment env = Environment.getEnvironment();
		String clientId = System.getProperty("ID", "Client#" + System.currentTimeMillis());
		System.out.println("clientId = " + clientId);


		Properties config = new Properties();
		config.setProperty("bootstrap.servers", env.kafkaURL);
		config.setProperty("group.id", "io.phaas.kafka_group_manager.Consumer");
		config.setProperty("client.id", clientId);
		config.setProperty("auto.offset.reset", "earliest");
		config.setProperty("enable.auto.commit", "false");
		config.setProperty("session.timeout.ms", "6000");
		config.setProperty("max.poll.records", "1500");
		config.setProperty("partition.assignment.strategy", "io.phaas.kafka_group_manager.ExclusiveAccessAssignor$ExclusiveAccessRangeAssignor");


		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer());
		consumer.subscribe(singleton("my-topic"), new ConsumerRebalanceListener() {
			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				System.out.println("Consumer.onPartitionsRevoked " + partitions);
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				System.out.println("Consumer.onPartitionsAssigned " + partitions);
			}
		});

		consumeMessages(consumer);
	}

	private static void consumeMessages(KafkaConsumer<String, String> consumer) {
		boolean verbose = true;
		while (true) {
			try {
				ConsumerRecords<String, String> poll = consumer.poll(10000);
				if (verbose) {
					Set<TopicPartition> assignment = consumer.assignment();
					System.out.println(assignment.stream()
							.map(tp -> tp + "@" + consumer.position(tp))
							.collect(Collectors.joining(" | ")));
					System.out.println("poll.count() = " + poll.count());
				}

				if (verbose) {
//					System.out.println(
//							StreamSupport.stream(poll.spliterator(), false)
//									.map(r -> r.value())
//									.collect(Collectors.joining(","))
//					);
				}

				consumer.commitSync();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
