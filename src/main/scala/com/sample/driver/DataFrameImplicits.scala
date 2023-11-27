package com.sample.driver

import com.databricks.spark.xml.schema_of_xml
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

trait DataFrameImplicits {

  def getInitialSchema(df: DataFrame, colName: String = "value") = {
    import df.sparkSession.implicits._
    schema_of_xml(df.select(colName).as[String])
  }

  def flattenFields(df: DataFrame) = {

    var mod_df = df

    mod_df.schema.foreach { c =>

      c.dataType.typeName.toLowerCase match {
        case "array" => mod_df = df.withColumn(c.name, explode(col(c.name)))
        case "struct" => mod_df = df.expandStrutType(c.name)
        case _ => None
      }

    }
    mod_df
  }


  def flattenFieldsFullTraverse(df: DataFrame): DataFrame = {

    var mod_df = df

    while (mod_df.schema.fields.map { x => x.dataType.typeName.toLowerCase }.exists { p => p == "struct" || p == "array" }) {
      mod_df = mod_df.transform(flattenFields)
    }
    mod_df

  }




  implicit class implicits(df:DataFrame){

    def expandStrutType( colName: String): DataFrame = {
      val cols = df.columns.map(s => if (s.equalsIgnoreCase(colName)) s"$s.*" else s)
      df.select(cols.map(col): _*)
    }


  }

}
