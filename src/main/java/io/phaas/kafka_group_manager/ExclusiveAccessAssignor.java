package io.phaas.kafka_group_manager;

import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;

/**
 * This partition assignor supports on-demand exclusive access to all partitions for
 * a single client, using a specially-crafted client Id. This allows clients
 * to join the group temporarily, manage the offsets for all partitions and then disconnect.
 * When the partitions are reassigned to the regular consumers by the coordinator, the new offsets
 * will be used.
 */
public class ExclusiveAccessAssignor implements PartitionAssignor {
	public static final String EXCLUSIVE_ACCESS_CLIENT_ID = "MAGIC";


	/**
	 * An ExclusiveAccessAssignor delegating to a {@link RangeAssignor}
	 *
	 * @see ExclusiveAccessAssignor
	 */
	public static class ExclusiveAccessRangeAssignor extends ExclusiveAccessAssignor {
		public ExclusiveAccessRangeAssignor() {
			super(new RangeAssignor());
		}
	}

	/**
	 * An ExclusiveAccessAssignor delegating to a {@link RoundRobinAssignor}
	 *
	 * @see ExclusiveAccessAssignor
	 */
	public static class ExclusiveAccessRoundRobinAssignor extends ExclusiveAccessAssignor {
		public ExclusiveAccessRoundRobinAssignor() {
			super(new RoundRobinAssignor());
		}
	}

	private final PartitionAssignor delegate;


	public ExclusiveAccessAssignor(PartitionAssignor delegate) {
		this.delegate = delegate;
	}

	@Override
	public Subscription subscription(Set<String> topics) {
		return delegate.subscription(topics);
	}

	@Override
	public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
		// There could be more tha one client matching this id. The first one in this list "wins"
		Optional<String> exclusiveClient = subscriptions.keySet().stream()
				.filter(id -> id.startsWith(EXCLUSIVE_ACCESS_CLIENT_ID))
				.findAny();

		if (exclusiveClient.isPresent()) {
			return assignAllPartitionsToClient(metadata, subscriptions, exclusiveClient.get());
		}

		return delegate.assign(metadata, subscriptions);
	}

	private Map<String, Assignment> assignAllPartitionsToClient(Cluster metadata, Map<String, Subscription> subscriptions, String clientId) {

		List<TopicPartition> allPartitions = new ArrayList<>();
		for (String topic : subscriptions.get(clientId).topics()) {
			for (int i = 0; i < metadata.partitionCountForTopic(topic); i++) {
				allPartitions.add(new TopicPartition(topic, i));
			}
		}

		// Update the exclusive client's assignment to include all partitions for the subscribed topics
		// Assign no partitions to all other clients

		// Note: It's possible that each client's list of subscribed topics is different. While
		// the exclusive access client is connected, other clients will temporarily have no
		// assigned topics, even though the client with exclusive access is not managing them.
		final Assignment empty = new Assignment(emptyList());
		Map<String, Assignment> assignment = subscriptions.keySet().stream()
				.collect(Collectors.toMap(
						identity(),
						c -> c.equals(clientId) ? new Assignment(allPartitions) : empty));

		return assignment;
	}

	@Override
	public void onAssignment(Assignment assignment) {
		delegate.onAssignment(assignment);
	}

	@Override
	public String name() {
		return ExclusiveAccessAssignor.class.getSimpleName() + "-" + delegate.name();
	}
}
