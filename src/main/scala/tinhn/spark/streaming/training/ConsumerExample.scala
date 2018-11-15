package tinhn.spark.streaming.training

import java.text.SimpleDateFormat

import org.slf4j.LoggerFactory
import java.util.{Calendar, UUID}

import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

// convert Java Properties to Scala Map
import scala.collection.JavaConverters._

/**
  * spark-submit --class tinhn.spark.streaming.training.ConsumerExample spark-kafka-consumer_2.3.2-1.0.jar  <brokers> <readtopic> <username> <password> <writetopic>
  */
object ConsumerExample extends Serializable {
  val log = LoggerFactory.getLogger(ConsumerExample.getClass.getName)

  case class CarModel(name: String, total: Int, date_time: String)

  var brokers = ""
  var writeTopic = ""
  var username = ""
  var password = ""

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: ConsumerExample <brokers> <topics> <username> <password>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <username> is an account for authentication with kafka cluster
           |  <password> is a password of this account for authentication
           |
        """.stripMargin)
      System.exit(1)
    }

    val SPARK_MASTER_DEFAULT = "local[3]"
    val KAFKA_RECURRENT_STREAMING = 5
    val APP_NAME = "Demo Kafka Consumer 2 Kafka"

    //val brokers = scala.util.Try(args(0)).getOrElse("localhost:9092")
    val Array(brokers, readtopic, username, password) = args

    //Get the Kafka broker node
    //brokers = scala.util.Try(args(0)).getOrElse("localhost:9092")

    //Get the topic for producer
    if (args.length >= 5)
      writeTopic = scala.util.Try(args(4)).get

    lazy val sparkConf = new SparkConf()
    sparkConf.setAppName(APP_NAME).setMaster(SPARK_MASTER_DEFAULT)

    //Create streaming context from Spark
    val streamCtx = new StreamingContext(sparkConf, Seconds(KAFKA_RECURRENT_STREAMING))
    val sparkCtx = streamCtx.sparkContext

    //Create Direct Kafka Streaming for Consumer
    val topicsSet = readtopic.split(",").toSet

    val props = new MyKafkaConfig(username, password).GetKafkaProperties(brokers)
    val kafkaParams = props.asScala

    val messages = KafkaUtils.createDirectStream[String, String](
      streamCtx,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )
    var lines = messages.map(record => (record.value))

    //Get the car brand name, example data like: "2018-11-09 11:04:45,Toyota"
    val words = lines.map(x => x.split(",")(1))

    //Calculate the total car brand name
    val dStream = words.map(carname => Tuple2(carname, 1))
    val wordCounts = dStream.reduceByKey(_ + _)

    //Print received data or data transformed from the received data
    wordCounts.print()

    //Retrieve each RDD and transform to create DataFrame
    if (StringUtils.isNoneBlank(writeTopic)) {
      val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      wordCounts.foreachRDD(rdd => {
        //Mapping the RDD in the stream to new RDD with CarModel class and convert to DataFrame
        val data = rdd.map(
          x => CarModel(x._1, x._2, formatter.format(Calendar.getInstance.getTime))
        )
        data.foreachPartition(pushInKafkaAgain)
      })
    }

    //Start the stream
    log.info("Starting Spark Streaming")
    streamCtx.start()

    log.info("Spark Streaming is running")
    streamCtx.awaitTermination()
  }

  def pushInKafkaAgain(items: Iterator[CarModel]): Unit = {

    //Create some properties
    val props = new MyKafkaConfig(username, password).GetKafkaProperties(brokers)

    val producer = new KafkaProducer[String, String](props)

    items.foreach(obj => {
      val key = UUID.randomUUID().toString().split("-")(0)
      val gson = new Gson()
      val value = gson.toJson(obj)

      val data = new ProducerRecord[String, String](writeTopic, key, value)

      println("--- topic: " + writeTopic + " ---")
      println("key: " + data.key())
      println("value: " + data.value() + "\n")

      producer.send(data)

    })
    producer.close()
  }
}
