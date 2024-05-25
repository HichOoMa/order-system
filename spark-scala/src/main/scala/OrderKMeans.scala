package org.apache.spark.examples.mllib

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{ArrayType, FloatType, StructField, StructType}

object OrderKMeans {
  def LocationKMeams(data: DataFrame): Unit = {

    // val conf = new SparkConf().setAppName("KMeansExample")
    // val sc = new SparkContext(conf)

    /**
     *
     * Example to test the cluster with CSV file data
     *
     * val locationData = spark.read
     * .option("header", "true") // If CSV has header
     * .csv("src/main/scala/driverlogs.csv")
     *
     * locationData.show()
     *
     * val data = locationData.withColumn("latitude", col("latitude").cast("double"))
     * .na.drop()
     *
     */
    val parseArray = udf((s: String) => {
      val floatArray = s.replaceAll("\\[|\\]", "").split(",").map(_.trim.toFloat)
      Vectors.dense(floatArray.map(_.toDouble)) // Convert to Vector of doubles
    })

    val  parsedDF = data
        .withColumn("location", parseArray(col("value")))

    val assembler = new VectorAssembler()
      .setInputCols(Array("location"))
      .setOutputCol("features")
    val assembledData = assembler.transform(parsedDF)

    val means = new KMeans()
      .setK(3)

    val model = means.fit(assembledData)
    model.save("target/org/apache/spark/KMeansExample/KMeansModel")
    println("================================================================================")
  }

  def parseToFloatArray(location: String): Array[Float] = {
    val cleanedStr = location.stripPrefix("[").stripSuffix("]")

    val strArray = cleanedStr.split(",")

    val floatArray = strArray.map(_.toFloat)
    floatArray.foreach(p => println(p))
    floatArray
  }

}
