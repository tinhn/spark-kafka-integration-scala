# spark-kafka-integration-scala
Spark Streaming 2.3.2 integration with Kafka 2.0.0 writing by Scala version 2.11


Application is implemented as a Spark Streaming process which reads data from the Kafka topic, split them into words, count the words and print to console. This demonstration have use CloudKarafka cluster service 


### Configure

All of the authentication settings can be config first, something like:

```
BROKERS=broker1:9094,broker2:9094,broker3:9094
USERNAME=<username>
PASSWORD=<password>
TOPIC=<kafka topic>
```

### Build

```
git clone
cd spark-kafka-integration-scala
mvn clean compile assembly:single
```
The application build and store at dist folder with name <spark-kafka-consumer_2.3.2-1.0.jar>

## Deploying
As with any Spark applications, spark-submit is used to launch application
```
cd dist
./bin/spark-submit \
 --class tinhn.spark.streaming.training.ConsumerExample \
 --master local[4] \
 spark-kafka-consumer_2.3.2-1.0.jar \ 
 <brokers> <readtopics> <username> <password>
```
