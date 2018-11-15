package tinhn.spark.streaming.training

import java.util.{Properties, UUID}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

class MyKafkaConfig(username: String, password: String) {

  val jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";"
  val jaasCfg = String.format(jaasTemplate, username, password)

  def GetKafkaProperties(brokers: String): Properties = {

    val serializer = classOf[StringSerializer].getName
    val deserializer = classOf[StringDeserializer].getName

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("group.id", "mylabs-consumer")
    props.put("client.id", "mylabs-" + UUID.randomUUID().toString())
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", deserializer)
    props.put("value.deserializer", deserializer)
    props.put("key.serializer", serializer)
    props.put("value.serializer", serializer)

    /*using SASL/SCRAM for authorize*/
    props.put("security.protocol", "SASL_SSL")
    props.put("sasl.mechanism", "SCRAM-SHA-256")
    props.put("sasl.jaas.config", jaasCfg)

    props
  }


}
