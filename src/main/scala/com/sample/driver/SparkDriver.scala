package com.sample.driver

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkDriver {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val packages = s"org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8,org.apache.kafka:kafka-clients:3.6.0"


  lazy val spark: SparkSession = {
    System.setProperty("hadoop.home.dir","""C:\hadoop""")
    val appName = "sparkStreamingKafka"
    val sparkSession = if (System.getProperty("os.name").toLowerCase().contains("win")) {
      SparkSession.builder().master("local[*]").appName(appName).config("spark.jars.packages",packages).getOrCreate()
    } else {
      SparkSession.builder().master("yarn").appName(appName).getOrCreate()
    }

    sparkSession.conf.set("spark.jars.packages",packages)
    sparkSession

  }


}
