package org.bigdata.streaming

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Replays historical Taobao UserBehavior CSV rows to Kafka as JSON.
 *
 * Arguments:
 *   0: input CSV
 *   1: Kafka topic
 *   2: bootstrap servers
 *   3: delay between messages in milliseconds
 *   4: maximum rows to send (0 means all rows)
 */
object UserBehaviorReplayProducer {

  private case class Event(
      userId: Long,
      itemId: Long,
      categoryId: Long,
      behaviorType: String,
      timestamp: Long)

  def main(args: Array[String]): Unit = {
    val input = arg(args, 0, "dataset_test/UserBehavior.csv")
    val topic = arg(args, 1, "taobao_behavior")
    val bootstrapServers = arg(args, 2, "localhost:9092")
    val delayMillis = arg(args, 3, "200").toLong
    val maxRows = arg(args, 4, "0").toLong

    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.RETRIES_CONFIG, "3")

    val producer = new KafkaProducer[String, String](properties)
    val reader = Files.newBufferedReader(Paths.get(input), StandardCharsets.UTF_8)
    val failures = new AtomicLong(0L)
    var sent = 0L
    var skipped = 0L

    try {
      var line = reader.readLine()
      while (line != null && (maxRows <= 0L || sent < maxRows)) {
        parse(line) match {
          case Some(event) =>
            val record = new ProducerRecord[String, String](
              topic,
              event.userId.toString,
              toJson(event))
            producer.send(record, new Callback {
              override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
                if (exception != null) {
                  failures.incrementAndGet()
                  System.err.println("[ERROR] Kafka send failed: " + exception.getMessage)
                }
              }
            })
            sent += 1L
            if (delayMillis > 0L) {
              Thread.sleep(delayMillis)
            }
          case None =>
            skipped += 1L
        }
        line = reader.readLine()
      }
      producer.flush()
    } finally {
      reader.close()
      producer.close()
    }

    if (failures.get() > 0L) {
      throw new IllegalStateException("Kafka replay failed for " + failures.get() + " messages")
    }
    println("[SUCCESS] Historical replay completed: sent=" + sent + ", skipped=" + skipped)
  }

  private def arg(args: Array[String], index: Int, defaultValue: String): String = {
    if (args.length > index && args(index).nonEmpty) args(index) else defaultValue
  }

  private def parse(line: String): Option[Event] = {
    val fields = line.split(",", -1).map(_.trim)
    if (fields.length != 5) {
      None
    } else {
      try {
        val behaviorType = fields(3).toLowerCase
        if (!Set("pv", "fav", "cart", "buy").contains(behaviorType)) {
          None
        } else {
          Some(Event(
            fields(0).toLong,
            fields(1).toLong,
            fields(2).toLong,
            behaviorType,
            fields(4).toLong))
        }
      } catch {
        case _: NumberFormatException => None
      }
    }
  }

  private def toJson(event: Event): String = {
    s"""{"user_id":${event.userId},"item_id":${event.itemId},"category_id":${event.categoryId},"behavior_type":"${event.behaviorType}","timestamp":${event.timestamp}}"""
  }
}
