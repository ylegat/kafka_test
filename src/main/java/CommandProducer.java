import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class CommandProducer {

    private static final Logger logger = LoggerFactory.getLogger(CommandProducer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        RecordMetadata recordMetadata = commandProducer().send(new ProducerRecord<>("command", 0, "userId", "payload"))
                                                         .get();

        commandProducer().send(new ProducerRecord<>("command", 1, "userId", "payload"))
                         .get();

        logger.info("message pushed on topic '{}' on offset {} at {}",
                    recordMetadata.topic(),
                    recordMetadata.offset(),
                    Instant.ofEpochMilli(recordMetadata.timestamp()));
    }

    private static Producer<String, String> commandProducer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(CLIENT_ID_CONFIG, "CommandProducer");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
