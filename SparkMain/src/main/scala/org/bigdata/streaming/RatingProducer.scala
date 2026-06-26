package org.bigdata.streaming

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import com.fasterxml.jackson.databind.ObjectMapper

import java.util.Properties
import scala.util.Random

/**
 * 评分数据生成器
 * 模拟实时产生评分数据并发送到 Kafka
 */
object RatingProducer {

  private val KAFKA_TOPIC = "ratings"
  private val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
  private val mapper = new ObjectMapper()
  private val random = new Random()

  case class Rating(userId: Int, movieId: Int, rating: Double, timestamp: Long)

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)

    println("开始生成评分数据...")

    for (i <- 1 to 100) {
      val rating = Rating(
        userId = random.nextInt(100) + 1,
        movieId = random.nextInt(100) + 1,
        rating = Math.round((random.nextDouble() * 4.5 + 0.5) * 10.0) / 10.0,
        timestamp = System.currentTimeMillis() / 1000
      )

      val json = mapper.writeValueAsString(rating)
      val record = new ProducerRecord[String, String](KAFKA_TOPIC, json)

      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception != null) {
            exception.printStackTrace()
          } else {
            println(s"发送成功: $json")
          }
        }
      })

      Thread.sleep(1000) // 每秒发送一条
    }

    producer.close()
    println("数据生成完成")
  }
}
