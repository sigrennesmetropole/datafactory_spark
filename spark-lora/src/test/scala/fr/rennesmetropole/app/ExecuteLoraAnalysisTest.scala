package fr.rennesmetropole.tools

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import fr.rennesmetropole.app.ExecuteLoraAnalysis

class ExecuteLoraAnalysisTest {

  @Test
  def ExecuteLoraAnalysisTest(): Unit = {
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
    val args = Array("Test")
    ExecuteLoraAnalysis.main(args) match {
      case Right(dfOutput) => assertEquals(dfOutput.drop("insertedDate").except(dfExpected.drop("insertedDate")).count(), 0)
    }
  }

}