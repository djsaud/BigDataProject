package com.techno.project.sparkstream


import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.slf4j.LoggerFactory

object SparkStreamer {

  val logger = LoggerFactory.getLogger(this.getClass)

  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }


  def main(args: Array[String]): Unit = {

    setupLogging()

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("KafkaToSpark")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true");

    // Initialize StreamingContext with a Batch interval of 5 seconds
    val ssc = new StreamingContext(conf, Seconds(5))

    val spark = SparkSession.builder().appName("JsonReader").getOrCreate()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "group.id" -> "kafkaRead")

    val topics = List("topic1").toSet

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    val kafkaKey = stream.map(record => record.key())
    val kafkaValue = stream.map(record => record.value())

    logger.info(s"The KEY IS: $kafkaKey and the VALUE IS: $kafkaValue")

    val dstr = stream.map(record => {
      (record.key, record.value)
    })
    dstr.print()

    val offsetRanges = Array(
      // topic, partition, inclusive starting offset, exclusive ending offset
      OffsetRange("topic1", 0, 0, 100)
    )

    //Iterate over Dstreams
    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val key=rdd.map(rec => rec.key())
        val value=rdd.map(recVal => recVal.value())
        print(s"The Key is $key and the Value is $value")

        //Identify the Offesets that we are working with
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition { iter =>
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }

        val streamDF = spark.read.json(rdd.map(record => (record.value.trim)))

        streamDF.printSchema()
        streamDF.createOrReplaceTempView("t2")

        //TODO:: remove after testing
        val result = spark.sql("select account_number from t2")
        result.show()


      }

    })

    ssc.start()
    ssc.awaitTermination()
  }

}
