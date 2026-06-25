package org.example.streaming;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.Random;

/**
 * 评分数据生成器
 * 模拟实时产生评分数据并发送到 Kafka
 */
public class RatingProducer {

    private static final String KAFKA_TOPIC = "ratings";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        System.out.println("开始生成评分数据...");

        for (int i = 0; i < 100; i++) {
            Rating rating = new Rating(
                    random.nextInt(100) + 1,  // userId
                    random.nextInt(100) + 1,  // movieId
                    Math.round((random.nextDouble() * 4.5 + 0.5) * 10.0) / 10.0,  // rating 0.5-5.0
                    System.currentTimeMillis() / 1000  // timestamp
            );

            String json = mapper.writeValueAsString(rating);
            ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, json);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.println("发送成功: " + json);
                }
            });

            Thread.sleep(1000);  // 每秒发送一条
        }

        producer.close();
        System.out.println("数据生成完成");
    }

    static class Rating {
        public int userId;
        public int movieId;
        public double rating;
        public long timestamp;

        public Rating(int userId, int movieId, double rating, long timestamp) {
            this.userId = userId;
            this.movieId = movieId;
            this.rating = rating;
            this.timestamp = timestamp;
        }
    }
}
