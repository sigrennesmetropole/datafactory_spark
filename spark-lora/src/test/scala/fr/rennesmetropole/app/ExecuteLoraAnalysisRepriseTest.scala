package fr.rennesmetropole.app

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.junit.Test

class ExecuteLoraAnalysisRepriseTest {

  @Test
  def ExecuteLoraAnalysisRepriseTest(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()

    val schemaInput = StructType(
      List(
        StructField("id", StringType, false),
        StructField("deveui", StringType, false),
        StructField("enabledon", StringType, false),
        StructField("timestamp", StringType, false),
        StructField("name", StringType, false),
        StructField("value", StringType, false),
        StructField("inserteddate", StringType, false),
        StructField("measurenatureid", StringType, false),
        StructField("unitid", StringType, false)
      )
    )


    val dfExpected = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaInput) // mandatory
      .option("delimiter", ",")
      .load("src/test/resources/Local/app/ExecuteLoraAnalysisTestExpected.csv")

    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-09-03")
    ExecuteLoraAnalysisReprise.main(args) match {
      case Right(dfOutput) =>
        println("done")
    }
  }

}