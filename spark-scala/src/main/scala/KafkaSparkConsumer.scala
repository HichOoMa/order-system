import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

object KafkaSparkConsumer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KafkaSparkConsumer").setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
//    val sparkContext = new SparkContext(sparkConf)

//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "localhost:9092",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "your_consumer_group_id",
//      "auto.offset.reset" -> "latest", // Specify the starting offset
//      "enable.auto.commit" -> (false: java.lang.Boolean)
//    )
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "localhost:9092",
      "subscribe" -> "driver-location"
    )


    val kafkaDF = spark
      .readStream
      .format("kafka")
      .options(kafkaParams)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)", "CAST(partition AS INT)", "CAST(offset AS LONG)", "CAST(timestamp AS TIMESTAMP)")

    val query = kafkaDF.writeStream
      .foreachBatch(
      (outputDf: DataFrame, bid: Long) => {
        // Process valid data frames only
        if (!outputDf.isEmpty && outputDf.count() > 100) {
          import org.apache.spark.examples.mllib.OrderKMeans._
          LocationKMeams(outputDf)
          Thread.sleep(300000)
        }
      }
    )
//      .outputMode("append")
//      .format("console")
      .start()

    query.awaitTermination()
    //    val kafkaStream = KafkaUtils.createDirectStream[String, String](
//      streamingContext,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
//    )

    //    var data : RDD[Row] = spark.sparkContext.parallelize(Seq(Row(48.04583076476158,29.330396902877702)))

//    import org.apache.spark.examples.mllib.OrderKMeans.LocationKMeams
//    kafkaStream.foreachRDD { rdd =>
//      if (!rdd.isEmpty()) {
//        println(s"Received ${rdd.count()} messages from Kafka")
//        rdd.foreach(record => {
//          val loc = parseToFloatArray(record.value())
//          val seq = Seq(Row.fromSeq(loc))
//          if (seq != null && seq.nonEmpty) {
//            val rdd2 = spark.sparkContext.parallelize(seq)
//            data = data.union(rdd2)
//            LocationKMeams(data, spark)
//          }
////          println(record.value())
//        }) // Process each message
//      }
//    }
  }

  def parseToFloatArray(location: String): Array[Float] = {
    val cleanedStr = location.stripPrefix("[").stripSuffix("]")

    val strArray = cleanedStr.split(",")

    val floatArray = strArray.map(_.toFloat)
    floatArray.foreach(p=>println(p))
    floatArray
  }
}
