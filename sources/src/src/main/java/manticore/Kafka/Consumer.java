package manticore.Kafka;

import manticore.Worker;
import manticore.WorkerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;

import java.time.Duration;
import java.util.*;

public class Consumer {
    public static final int REBALANCING_TIMEOUT = 60000;
    public KafkaConsumer<String, String> consumer;

    protected boolean rebalancingState = false;
    protected long lastRebalancing;
    private List<String> topics;

    private int failedCommitCount = 0;

    // The worker's offset map (partition -> next offset to consume). Entries for partitions
    // revoked from this consumer must be dropped: otherwise the worker keeps committing the
    // last offset it saw for them, overwriting the new owner's commits with a stale value.
    private Map<TopicPartition, OffsetAndMetadata> trackedOffsets;

    public Consumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
        this.topics = new ArrayList<>();
    }

    public Consumer(WorkerConfig config) {
        lastRebalancing = System.currentTimeMillis();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", config.getInputHost());
        props.setProperty("group.id", config.getInputGroupName());
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("max.poll.records", Integer.toString(config.getMaxBatchSize()));
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.setProperty("fetch.min.bytes", Integer.toString(config.getKafkaFetchMinBytes()));
        props.setProperty("fetch.max.wait.ms", Integer.toString(config.getKafkaFetchMaxWaitMs()));
        props.setProperty("fetch.max.bytes", Integer.toString(config.getKafkaFetchMaxBytes()));
        props.setProperty("max.poll.records", Integer.toString(config.getKafkaMaxPollRecords()));

        this.consumer = new KafkaConsumer<>(props);
        List<String> topics = Arrays.asList(config.getInputTopic().split(","));
        subscribe(topics);
        this.topics = topics;
    }

    protected void subscribe(List<String> topics) {
        this.consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                rebalancingState = true;
                lastRebalancing = System.currentTimeMillis();
                forgetPartitions(collection);
                Worker.getLogger().info("[KafkaConsumer] Revoked partitions {} for topics {}", collection, topics);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                rebalancingState = false;
                Worker.getLogger().info("[KafkaConsumer] Assigned partitions {} for topics {}", collection, topics);
            }
        });
    }

    public ConsumerRecords<String, String> consume() {
        synchronized (this) {
            return this.consumer.poll(Duration.ofMillis(100));
        }
    }

    public java.util.Set<org.apache.kafka.common.TopicPartition> assignment() {
        synchronized (this) {
            return this.consumer.assignment();
        }
    }

    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        synchronized (this) {
            return this.consumer.endOffsets(partitions);
        }
    }

    public long position(TopicPartition partition) {
        synchronized (this) {
            return this.consumer.position(partition);
        }
    }

    public void commitSync() {
        synchronized (this) {
            this.consumer.commitSync();
        }
    }

    public void trackOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        synchronized (this) {
            this.trackedOffsets = offsets;
        }
    }

    // Called from the rebalance listener, i.e. inside poll(), with this object's lock held.
    protected void forgetPartitions(Collection<TopicPartition> partitions) {
        if (trackedOffsets != null && partitions != null && !partitions.isEmpty()) {
            trackedOffsets.keySet().removeAll(partitions);
        }
    }

    // Only commit offsets of partitions this consumer currently owns.
    protected Map<TopicPartition, OffsetAndMetadata> ownedOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        Set<TopicPartition> assigned = this.consumer.assignment();
        Map<TopicPartition, OffsetAndMetadata> owned = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            if (assigned.contains(entry.getKey())) {
                owned.put(entry.getKey(), entry.getValue());
            }
        }
        if (owned.size() < offsets.size()) {
            Worker.getLogger().debug("[KafkaConsumer] Skipping commit for {} partition(s) not assigned to this consumer",
                    offsets.size() - owned.size());
        }
        return owned;
    }

    public void commitSync(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        synchronized (this) {
            Map<TopicPartition, OffsetAndMetadata> owned = ownedOffsets(currentOffsets);
            if (owned.isEmpty()) {
                return;
            }
            try {
                this.consumer.commitSync(owned, Duration.ofMillis(5000L));
                failedCommitCount = 0;
                Worker.getLogger().debug("[KafkaConsumer] Successfully committed offsets for topics");
            } catch (TimeoutException exception) {
                failedCommitCount++;
                if (failedCommitCount < 10) {
                    Worker.getLogger().info("[KafkaConsumer] Retrying commit for offsets (attempt {}/10)", failedCommitCount);
                    this.commitSync(currentOffsets);
                } else {
                    Worker.getLogger().error("[KafkaConsumer] Failed to commit offsets after 10 attempts: {}", exception.getMessage());
                    throw exception;
                }
            }
        }
    }

    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        synchronized (this) {
            Map<TopicPartition, OffsetAndMetadata> owned = ownedOffsets(currentOffsets);
            if (owned.isEmpty()) {
                return;
            }
            this.consumer.commitAsync(owned, null);
        }
    }

    public void close() {
        this.consumer.close();
    }

    public Boolean checkIsAtRebalancingState() {
        return rebalancingState && lastRebalancing < System.currentTimeMillis() - Consumer.REBALANCING_TIMEOUT;
    }

    public void reassign() {
        synchronized (this) {
            this.consumer.unsubscribe();
            subscribe(this.topics);
            Worker.getLogger().info("[KafkaConsumer] Triggered reassignment due to prolonged lag=0");
        }
    }
}
