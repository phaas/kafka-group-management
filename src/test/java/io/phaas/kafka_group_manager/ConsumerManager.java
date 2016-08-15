package io.phaas.kafka_group_manager;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singleton;

/**
 * Created by patrick on 8/13/2016.
 */
public class ConsumerManager {

	public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
		Environment env = Environment.getEnvironment();

		Properties config = new Properties();
		config.setProperty("bootstrap.servers", env.kafkaURL);
		config.setProperty("group.id", "io.phaas.kafka_group_manager.Consumer");
		config.setProperty("auto.offset.reset", "earliest");
		config.setProperty("enable.auto.commit", "false");
		config.setProperty("session.timeout.ms", "6000");
		config.setProperty("partition.assignment.strategy", "io.phaas.kafka_group_manager.ExclusiveAccessAssignor$ExclusiveAccessRangeAssignor");
		config.setProperty("client.id", "MAGIC");

		manageGroup(config, seekAssignmentTo(100));
//		manageGroup(config, pauseFor(Duration.ofSeconds(90)));
	}

	public static java.util.function.Consumer<KafkaConsumer<?, ?>> pauseFor(Duration duration) {
		return c -> {
			c.pause(c.assignment());

			long end = System.currentTimeMillis() + duration.toMillis();

			while (System.currentTimeMillis() < end) {
				c.poll(1000);
			}
		};
	}

	public static java.util.function.Consumer<KafkaConsumer<?, ?>> seekAssignmentTo(long position) {
		return consumer -> {
			// Seek to the desired offset
			for (TopicPartition topicPartition : consumer.assignment()) {
				consumer.seek(topicPartition, 0);
			}

			consumer.commitSync();
			// Commit the new offsets; Ignore the results (and don't ack the messages that we received)
			consumer.pause(consumer.assignment());
			ConsumerRecords<?, ?> poll = consumer.poll(position);
			System.out.println("poll.count() = " + poll.count());


			for (TopicPartition partition : consumer.assignment()) {
				System.out.println("partition = " + partition + " = " + consumer.position(partition));
			}

			// Our job is done; offsets have been reset.
		};
	}

	private static void manageGroup(Properties config, java.util.function.Consumer<KafkaConsumer<?, ?>> callback) {
		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer());

		CompletableFuture<Collection<TopicPartition>> future = new CompletableFuture<>();
		consumer.subscribe(singleton("my-topic"), new ConsumerRebalanceListener() {
			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				System.out.println("Consumer.onPartitionsRevoked " + partitions);
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				System.out.println("Consumer.onPartitionsAssigned " + partitions);
				future.complete(partitions);
			}
		});

		// Start up the consumer. Ignore the results, if any
		consumer.poll(0);

		// Wait for the consumer to have partitions assigned
		Collection<TopicPartition> partitions = future.join();

		try {
			callback.accept(consumer);
		} finally {
			consumer.close();
		}
	}
}
