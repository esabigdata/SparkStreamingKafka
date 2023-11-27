package com.sample.stream

import com.sample.driver.SparkDriver
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.Dataset
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
object SparkKafaConsumerV2 extends SparkDriver with App {

  import spark.implicits._


  val checkPointPath = ""
  val dataPath = ""
  val path_to_offset_manager = " "
  val topicName = "quickstart-events"

  val latest_offsets = spark.read.format("parquet").load(path_to_offset_manager).filter(s"topic = '$topicName'")
    .groupBy("partition").agg(max("offset").as("offset")).rdd.map{f => (f.getInt(0),f.getInt(1))}.collect()
    .map{f => s""" "${f._1}" : ${f._2} """}.mkString(",")

  val latest_offset_json = s""" { "$topicName" : { ${ latest_offsets} """

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "quickstart-events")
    .option("includeHeaders", "false")
    .option("startingOffsets", latest_offset_json)
    .load()
    .withColumn("key", $"key".cast(StringType))
    .withColumn("value", $"value".cast(StringType))


  df.withColumn("load_date", to_date($"timestamp").cast(StringType))
    .withColumn("hour", to_date($"timestamp").cast(StringType))
    .writeStream.option("checkpoint", checkPointPath)
    .trigger(Trigger.Continuous("10 seconds"))
    .foreachBatch{(rows,batchId) =>

    rows.write.mode("append").partitionBy("load_date", "hour").parquet(dataPath)

      //alternatively can use NoSql Database as well
    rows.select($"partition",$"offset",lit(topicName).as("topic")).write.mode("append")
      .partitionBy("topic","partition").parquet(path_to_offset_manager)


    }.start()


}
