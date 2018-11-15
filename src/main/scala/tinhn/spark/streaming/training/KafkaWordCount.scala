package tinhn.spark.streaming.training

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  *
  * Example:
  *    $ ./bin/spark-submit --class tinhn.spark.streaming.training.KafkaWordCount spark-kafka-consumer_2.3.2-1.0.jar
  */
object KafkaWordCount {
  val log = LoggerFactory.getLogger(KafkaWordCount.getClass.getName)

  def main(args: Array[String]) {

    val brokers ="localhost:9092";

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("Kafka Word Count").setMaster("local[4]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(2))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "mygroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")
    val messages = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)
//    lines.foreachRDD( rdd => {
//        rdd.foreach(x =>{
//          log.info(x)
//        })
//    })

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    log.info("Streaming running")
    streamingContext.start()

    log.info("Streaming stated")
    streamingContext.awaitTermination()
  }
}
