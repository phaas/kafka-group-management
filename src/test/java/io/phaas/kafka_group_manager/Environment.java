package io.phaas.kafka_group_manager;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Created by patrick on 8/13/2016.
 */
public class Environment {

	public final String kafkaURL;
	public final int kafkaPort;
	public final String zookeeperURL;
	public final int zookeeperPort;
	public final File dataDirectory;
	public final String nodeId;
	public final List<String> allTopicsToCreate;

	public Environment(String kafkaURL, int kafkaPort, String zookeeperURL, int zookeeperPort,
	                   String nodeId, List<String> allTopicsToCreate) {
		this.kafkaURL = kafkaURL;
		this.kafkaPort = kafkaPort;
		this.zookeeperURL = zookeeperURL;
		this.zookeeperPort = zookeeperPort;
		this.nodeId = nodeId;
		this.allTopicsToCreate = allTopicsToCreate;
		dataDirectory = new File("target/");
	}

	public static final int DCA_DBC_PARTITIONS = 2;
	private static final Environment DEFAULT = new Environment(
			"localhost:9092", 9092, "localhost:2181", 2181, "01",
			Arrays.asList("my-topic@10"));


	public static Environment getEnvironment() {
		switch (System.getProperty("ENV", "")) {
			default:
				return DEFAULT;
		}
	}
}
