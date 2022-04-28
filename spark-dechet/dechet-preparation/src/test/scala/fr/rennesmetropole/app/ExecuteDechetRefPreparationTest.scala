package fr.rennesmetropole.app

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._

class ExecuteDechetRefBacPreparationTest {

  @Test
  def ExecuteDechetRefBacPreparationTest(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()

    val schemaOutput = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("categorie_recipient", StringType, false),
          StructField("type_recipient", StringType, false),
          StructField("litrage_recipient", StringType, false),
          StructField("code_puce", StringType, false),
          StructField("frequence_om", IntegerType, true),
          StructField("frequence_cs", DoubleType, true),  //FloatType
          StructField("date_photo", StringType, false)
        )
      )

    val dfExpected = spark
      .read
      .option("header", "false")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutput) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetExutoirePreparation/Output/ExecuteDechetRefPreparationTestOutput.csv")

    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2021-01-01")
    ExecuteDechetRefPreparation.main(args) match {
    case Right(dfPrepared) => assertEquals(0, dfPrepared.except(dfExpected).count())
    }
  }
}

class ExecuteDechetRefProdPreparationTest {

  @Test
  def ExecuteDechetRefProdPreparationTest(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()

    val schemaOutput = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("rva_adr", IntegerType, true),
          StructField("code_insee", IntegerType, true),
          StructField("nom_commune", StringType, false),
          StructField("type_producteur", StringType, true),
          StructField("activite", StringType, false),
          StructField("longitude", FloatType, true),
          StructField("latitude", FloatType, true),
          StructField("date_photo", StringType, false) 
        )
      )


    val dfExpected = spark
      .read
      .option("header", "false")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutput) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetExutoirePreparation/Output/ExecuteDechetExutoirePreparationTestOutput.csv")

    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2021-01-01")
    ExecuteDechetRefPreparation.main(args) match {
    case Right(dfPrepared) => assertEquals(0, dfPrepared.except(dfExpected).count())
    }
  }

}