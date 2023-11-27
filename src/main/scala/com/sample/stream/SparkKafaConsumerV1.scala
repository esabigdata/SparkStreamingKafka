package com.sample.stream

import com.sample.driver.SparkDriver
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType

object SparkKafaConsumerV1 extends SparkDriver with App {

  import spark.implicits._

  val checkPointPath = ""
  val dataPath = ""

  val df = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "quickstart-events")
    .option("includeHeaders", "false")
    .option("startingOffsets","latest")
    .load()
    .withColumn("key",$"key".cast(StringType))
    .withColumn("value", $"value".cast(StringType))

  df.printSchema()


  df.withColumn("load_date",to_date($"timestamp").cast(StringType))
    .withColumn("hour",to_date($"timestamp").cast(StringType))
    .writeStream.format("parquet").partitionBy("load_date","hour").trigger(Trigger.Continuous("10 seconds"))
    .option("checkpoint",checkPointPath).start(dataPath)


}
