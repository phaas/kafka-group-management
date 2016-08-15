package io.phaas.kafka_group_manager;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import scala.collection.Iterator;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.Properties;

import static kafka.admin.AdminUtils.createTopic;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by patrick on 8/13/2016.
 */
public class Launcher {

	private static final Logger LOGGER = getLogger(Launcher.class);

	public static void main(String[] args) throws IOException, InterruptedException, SQLException {
		final Environment env = Environment.getEnvironment();

		File zkSnapshotDir = mkdirs(env.dataDirectory, "test-zookeeper-snapshots");
		File zkLogDir = mkdirs(env.dataDirectory, "test-zookeeper-logs");
		File kafkaDir = mkdirs(env.dataDirectory, "test-kafka");

		LOGGER.info("ZK snapshotDir: {}", zkSnapshotDir.getAbsolutePath());
		LOGGER.info("ZK logDir: {}", zkLogDir.getAbsolutePath());
		LOGGER.info("Kafka logDir: {}", kafkaDir.getAbsolutePath());

		int tickTime = 500;
		ZooKeeperServer zkServer = new ZooKeeperServer(zkSnapshotDir, zkLogDir, tickTime);
		ServerCnxnFactory zookeeper = NIOServerCnxnFactory.createFactory();
		zookeeper.configure(new InetSocketAddress("localhost", env.zookeeperPort), 16);
		zookeeper.startup(zkServer);

		Properties kafkaConfig = new Properties();
		kafkaConfig.setProperty("zookeeper.connect", env.zookeeperURL);
		kafkaConfig.setProperty("broker.id", "1");
		kafkaConfig.setProperty("delete.topic.enable", "true");
		kafkaConfig.setProperty("host.name", "localhost");
		kafkaConfig.setProperty("port", Integer.toString(env.kafkaPort));
		kafkaConfig.setProperty("log.dir", kafkaDir.getAbsolutePath());
		kafkaConfig.setProperty("log.flush.interval.messages", String.valueOf(1));
		KafkaServerStartable kafka = new KafkaServerStartable(new KafkaConfig(kafkaConfig));
		kafka.startup();

		System.out.println("Creating topics");
		createTopics(env);

		waitForUser("Press enter to shut down");
		kafka.shutdown();
		kafka.awaitShutdown();
        zookeeper.shutdown();
	}


	public static void waitForUser(String prompt) {
		try {
			System.in.skip(System.in.available());
			System.out.println(prompt);
			System.in.read();
		} catch (IOException e) {
		}
	}

	private static File mkdirs(File dataDirectory, String child) {
		File kafkaDir = new File(dataDirectory, child);
		kafkaDir.mkdirs();
		return kafkaDir;
	}

	public static void createTopics(Environment env) {
		ZkClient zkClient = new ZkClient("localhost:" + env.zookeeperPort, 5000, 5000, ZKStringSerializer$.MODULE$);
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection("localhost:" + env.zookeeperPort), false);

		final Iterator<String> it = AdminUtils.fetchAllTopicConfigs(zkUtils).keysIterator();
		while (it.hasNext()) {
			String next = it.next();
			System.out.println("Topic: " + next);
		}

		for (String topic : env.allTopicsToCreate) {
			final String[] split = topic.split("@");
			createTopicIfNotExists(zkUtils, split[0], Integer.valueOf(split[1]));
		}
	}

	private static void createTopicIfNotExists(ZkUtils zkUtils, String topic, int partitions) {
		if (AdminUtils.topicExists(zkUtils, topic)) {
			LOGGER.info("Topic already exists: {}", topic);
		} else {
			LOGGER.info("Creating topic: {}", topic);
			createTopic(zkUtils, topic, partitions, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
		}
	}
}
