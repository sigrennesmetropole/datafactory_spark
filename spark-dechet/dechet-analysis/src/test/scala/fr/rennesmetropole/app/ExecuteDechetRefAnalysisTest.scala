package fr.rennesmetropole.app

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._

class ExecuteDechetRefAnalysisTest {

  @Test
  def ExecuteDechetRefnalysisTest(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()

    val schemaOutput = StructType(
      List(
        StructField("Code_puce", StringType, true),
        StructField("Pesee_net", DoubleType, true),
        StructField("Statut_de_la_levee", IntegerType, false),
        StructField("Latitude", DoubleType, true),
        StructField("Longitude", DoubleType, true),
        StructField("date_mesure", StringType, true),
        StructField("year", StringType, true),
        StructField("month", StringType, true),
        StructField("day", StringType, true),
        StructField("technical_key", StringType, true)
      )
    )


    val dfExpected = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutput) // mandatory
      .option("delimiter", ";")
      .csv("src/test/resources/Local/app/ExecuteDechetAnalysis/Output/ExecuteDechetRefAnalysisTestOutput.csv")

    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2021-01-01")
    ExecuteDechetAnalysis.main(args) match {
    case Right(dfPartitioned) => {
     /* println("dfPartitioned")
      dfPartitioned.show(false)
      dfExpected.show(false)*/
      assertEquals(0, dfPartitioned.except(dfExpected).count())
    }
  }
  }

}