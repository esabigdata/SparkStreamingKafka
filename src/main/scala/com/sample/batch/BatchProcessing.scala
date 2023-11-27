package com.sample.batch

import com.databricks.spark.xml.functions.from_xml
import com.sample.driver.{DataFrameImplicits, SparkDriver}
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import java.util.Calendar

object BatchProcessing extends SparkDriver with DataFrameImplicits with App{

  import spark.implicits._

  val raw_data_path = ""
  val processed_data_path = ""
  val applyMergeSmallFiles:Boolean = false


  val last_load_date,last_hour = spark.sql("select max(load_date) as last_load_date,max(hour) as last_hour from default.offset_logger_metrics where process_status = 'completed' ").take(1).map{f => (f.getString(0),f.getInt(1))}


  val df = spark.read.format("parquet").load(raw_data_path)
    .filter($"load_date" >= last_load_date && $"hour" > last_hour && $"hour" < hour(current_timestamp()) )
    .dropDuplicates(Array("partition","offset")).cache()

  val log_stats = df.groupBy($"load_date",$"hour",$"partition".as("kfka_part"))
    .agg(min($"offset").as("strt_offset")
      ,max($"offset").as("end_offset")
      ,min($"timestamp").as("strt_tmpst")
      , max($"timestamp").as("end_tmpst")
      ,count($"timestamp").as("cnt")).withColumn("process_status",lit("initiated")).cache()

  write_to_log_tbl(log_stats)
  mergeSmallFilesStreaming(df,log_stats.select("hour").distinct().count().toInt)

  process_data(df)

  write_to_log_tbl(log_stats.withColumn("process_status",lit("completed")))

  df.unpersist()
  log_stats.unpersist()




  def process_data(df:DataFrame,upsert:Boolean=false) = {

    //  val xml_schema = getInitialSchema(df.select("value"))
    val schema = new StructType().add("book", StructType(Array(
      StructField("book_id", StringType, true),
      StructField("author", StringType, true),
      StructField("description", StringType, true),
      StructField("genre", StringType, true),
      StructField("price", DoubleType, true),
      StructField("publish_date", DateType, true),
      StructField("title", StringType, true)
    )))

    val clean_df = df.select(concat_ws("-",$"load_date",$"partition",$"offset",$"timestamp").as("source_key"),
      from_xml($"value", schema).as("value")).withColumn("value",$"value.*").transform(flattenFieldsFullTraverse)

    if(upsert){
      clean_df.cache()
      val publish_dates = clean_df.select("publish_date").distinct().map(_.getString(0)).collect().mkString("'","','","'")
      var old_data = spark.read.format("parquet").load(processed_data_path).filter(expr(s"publish_date in ($publish_dates)"))
      old_data = old_data.join(clean_df.select($"book_id".as("book_id_new")),$"book_id"===$"book_id_new","left_outer").filter($"book_id_new".isNull).drop("book_id_new")

      val union_all_df = old_data.union(clean_df)

      union_all_df.repartition(20,$"publish_date").write.mode("overwrite").partitionBy("publish_date").parquet(processed_data_path)
      clean_df.unpersist()
    }else{
      clean_df.repartition(2,$"publish_date").write.mode("append").partitionBy("publish_date").parquet(processed_data_path)

      /*
      At end of day merge all files in current partition
       */
      if(applyMergeSmallFiles && Calendar.getInstance().get(Calendar.HOUR_OF_DAY) == 23){
        val current_date = java.time.LocalDate.now.toString
        val today_data = spark.read.format("parquet").load(processed_data_path).filter($"publish_date" === current_date)
        today_data.coalesce(1).write.mode("overwrite").parquet("/tmp/processed_data")
        spark.read.format("parquet").load("/tmp/processed_data")
          .repartition(1,$"publish_date").write.mode("overwrite").partitionBy("publish_date").parquet(processed_data_path)
      }
    }
  }

  def mergeSmallFilesStreaming(df:DataFrame,num_of_partitions:Int=1) = {
    /*
    merging small files in the streaming path
     */
    if(applyMergeSmallFiles) {
      df.coalesce(num_of_partitions).write.mode("overwrite").parquet("/tmp/raw_data_path")
      spark.read.format("parquet").load("/tmp/raw_data_path").repartition(num_of_partitions, $"load_date", $"hour").write.partitionBy("load_date", "hour").mode("overwrite").parquet(raw_data_path)
    }

  }

  def write_to_log_tbl(df: DataFrame) =
    df.select("kfka_part", "strt_offset", "end_offset", "strt_tmpst", "end_tmpst", "cnt", "process_status")
      .withColumn("last_uptd_by",lit("BatchProcessing")).withColumn("last_uptd_date",current_timestamp())
      .write.mode("append").insertInto("default.offset_logger")



}
