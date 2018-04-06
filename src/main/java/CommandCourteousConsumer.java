import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class CommandCourteousConsumer {

    private static final Logger logger = LoggerFactory.getLogger(CommandCourteousConsumer.class);

    private static volatile long lastPoll = 0;

    public static void main(String[] args) {
        Consumer<String, String> consumer = consumer();
        consumer.subscribe(singleton("command"), consumerRebalanceListener(consumer));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            records.iterator().forEachRemaining(consumerRecord -> {
                lastPoll = System.currentTimeMillis();

                logger.info("message pulled from topic '{}' on offset {} at {}",
                            consumerRecord.topic(),
                            consumerRecord.offset(),
                            Instant.ofEpochMilli(consumerRecord.timestamp()));

                try {
                    logger.info("waiting...");
                    TimeUnit.MINUTES.sleep(20);
                    logger.info("woke up");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            consumer.commitSync();
        }
    }

    private static ConsumerRebalanceListener consumerRebalanceListener(Consumer<String, String> consumer) {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                logger.info("on partition revoked: {}", partitions);
                consumer.commitSync();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                logger.info("on partition assigned: {}", partitions);
            }
        };
    }

    private static Consumer<String, String> consumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(GROUP_ID_CONFIG, "CommandConsumer");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(MAX_POLL_INTERVAL_MS_CONFIG, 10000);

        return new KafkaConsumer<>(props);
    }
}
