package io.phaas.kafka_group_manager;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by patrick on 8/13/2016.
 */
public class Producer {

	public static void main(String[] args) {
		Environment env = Environment.getEnvironment();

		Properties config = new Properties();
		config.setProperty("bootstrap.servers", env.kafkaURL);
		config.setProperty("acks", "all");

		final KafkaProducer<String, String> producer = new KafkaProducer<>(config,
				new StringSerializer(),
				new StringSerializer());


		long count = 0;
		while (true) {
			String now = String.valueOf(System.currentTimeMillis());

			if (++count % 100 == 0) {
				System.out.println("Sending # " + count);
			}

			Future<RecordMetadata> future = producer.send(new ProducerRecord<>("my-topic", now, now));
			try {
				future.get();
				Thread.sleep(100);
			} catch (InterruptedException | ExecutionException e) {
				System.out.println("Failed to send message " + now);
				e.printStackTrace();
			}
		}


	}
}
