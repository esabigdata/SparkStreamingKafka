package com.sample.stream

import com.sample.driver.SparkDriver
import com.databricks.spark.xml
object SparkKafaProducer extends SparkDriver with App{

  val columnsNames = Seq("empId","firstName", "LastName","salary","age")
  val data = Seq(
    ("1","John","John","100000","40"),
    ("2", "John", "John", "100000", "40"),
    ("3", "John", "John", "100000", "40"),
    ("4", "John", "John", "100000", "40"))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(data)).toDF(columnsNames:_*)



  df.selectExpr("to_json(struct(*)) AS value").repartition()
    .write
    .format("kafka")
    .option("kafka.batch.size", 10000)
    .option("kafka.request.timeout.ms", 100000)
    .option("checkpointLocation", "/mnt/telemetry/cp.txt")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "josn_data_topic")
    .save()


}
