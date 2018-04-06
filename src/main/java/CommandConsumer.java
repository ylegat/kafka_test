import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.stream.StreamSupport;

import static java.util.Collections.singleton;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.maxBy;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class CommandConsumer {

    private static final Logger logger = LoggerFactory.getLogger(CommandConsumer.class);

    public static void main(String[] args) {
        Producer<String, String> eventProducer = eventProducer();
        eventProducer.initTransactions();

        Consumer<String, String> consumer = commandConsumer();
        consumer.subscribe(singleton("command"), consumerRebalanceListener(consumer));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            if (records.isEmpty()) {
                continue;
            }

            eventProducer.beginTransaction();

            Map<TopicPartition, OffsetAndMetadata> map = StreamSupport.stream(records.spliterator(), false)
                                                                      .peek(consumerRecord -> {
                                                                          logger.info("message pulled from topic '{}' on offset {} at {}",
                                                                                      consumerRecord.topic(),
                                                                                      consumerRecord.offset(),
                                                                                      Instant.ofEpochMilli(consumerRecord.timestamp()));

                                                                          eventProducer().send(new ProducerRecord<>("event", 1, "userId", "payload"));
                                                                      })
                                                                      .map(record -> new Object() {
                                                                          TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                                                                          OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1);
                                                                      })
                                                                      .collect(groupingBy(o -> o.topicPartition,
                                                                                          collectingAndThen(maxBy(comparingLong(o -> o.offsetAndMetadata.offset())),
                                                                                                            maxOffset -> maxOffset.get().offsetAndMetadata)));

            eventProducer.sendOffsetsToTransaction(map, "CommandConsumer");
            eventProducer.commitTransaction();
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> test(Map<TopicPartition, OffsetAndMetadata> map,
                                                               ConsumerRecord<String, String> consumerRecord) {
        map.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset() + 1));
        return map;
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

    private static Consumer<String, String> commandConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(GROUP_ID_CONFIG, "CommandConsumer");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(MAX_POLL_INTERVAL_MS_CONFIG, 10000);

        return new KafkaConsumer<>(props);
    }

    private static Producer<String, String> eventProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(CLIENT_ID_CONFIG, "CommandConsumer");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

}
