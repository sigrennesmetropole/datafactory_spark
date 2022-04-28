package fr.rennesmetropole.app

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._

class ExecuteDechetExutoireAnalysisTest {

  @Test
  def ExecuteDechetExutoireAnalysisTest(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()

    val schemaOutput = StructType(
      List(
        StructField("code_immat", StringType, false), //Immat. 
        StructField("date_mesure", StringType, true), //Date service véhic. 
        StructField("code_tournee", IntegerType, true), //Description tournée 
        StructField("distance", IntegerType, false), //Km réalisé 
        StructField("secteur", StringType, true), //LOT 
        StructField("nb_bac", IntegerType, false), //Service 
        StructField("localisation_vidage", StringType, true), //Nom rech. lieu de vidage 
        StructField("poids", DoubleType, true), //Nom multiples ligne 
        StructField("type_vehicule", FloatType, true), //Clé unique Ligne ticket 
        StructField("date_crea", StringType, false),
        StructField("date_modif", StringType, false),
        StructField("year", StringType, false),
        StructField("month", StringType, false),
        StructField("day", StringType, false)
        )
    )


    val dfExpected = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutput) // mandatory
      .option("delimiter", ";")
      .csv("src/test/resources/Local/app/ExecuteDechetExutoireAnalysis/Output/ExecuteDechetExutoireAnalysisTestOutput.csv")

    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2021-01-01")
    ExecuteDechetExutoireAnalysis.main(args) match {
    case Right(dfPartitioned) => {
     /* println("dfPartitioned")
      dfPartitioned.show(false)
      dfExpected.show(false)*/
      assertEquals(0, dfPartitioned.except(dfExpected).count())
    }
  }
  }

}