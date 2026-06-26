package org.example.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Replays Taobao UserBehavior CSV rows to Kafka as JSON messages.
 */
public class UserBehaviorProducer {

    private static final String DEFAULT_TOPIC = "taobao_behavior";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_INPUT = "dataset_test/UserBehavior.csv";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String input = args.length > 0 ? args[0] : DEFAULT_INPUT;
        String topic = args.length > 1 ? args[1] : DEFAULT_TOPIC;
        String bootstrapServers = args.length > 2 ? args[2] : DEFAULT_BOOTSTRAP_SERVERS;
        long delayMillis = args.length > 3 ? Long.parseLong(args[3]) : 200L;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = Files.newBufferedReader(Path.of(input))) {
            String line;
            long sent = 0;
            while ((line = reader.readLine()) != null) {
                UserBehavior event = parse(line);
                if (event == null) {
                    continue;
                }
                String json = mapper.writeValueAsString(event);
                producer.send(new ProducerRecord<>(topic, String.valueOf(event.user_id), json));
                sent++;
                if (delayMillis > 0) {
                    Thread.sleep(delayMillis);
                }
            }
            producer.flush();
            System.out.println("UserBehavior replay completed, sent rows: " + sent);
        }
    }

    private static UserBehavior parse(String line) throws IOException {
        String[] parts = line.split(",", -1);
        if (parts.length != 5) {
            return null;
        }
        try {
            return new UserBehavior(
                    Long.parseLong(parts[0]),
                    Long.parseLong(parts[1]),
                    Long.parseLong(parts[2]),
                    parts[3],
                    Long.parseLong(parts[4])
            );
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    static class UserBehavior {
        public long user_id;
        public long item_id;
        public long category_id;
        public String behavior_type;
        public long timestamp;

        UserBehavior(long userId, long itemId, long categoryId, String behaviorType, long timestamp) {
            this.user_id = userId;
            this.item_id = itemId;
            this.category_id = categoryId;
            this.behavior_type = behaviorType;
            this.timestamp = timestamp;
        }
    }
}
