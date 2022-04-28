package fr.rennesmetropole.app

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._

class ExecuteDechetExutoirePreparationTest {

  @Test
  def ExecuteDechetExutoirePreparationTest(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()

    val schemaOutput = StructType(
      List(
        StructField("immat", StringType, false), //Immat. 
        StructField("date_service_vehic", StringType, true), //Date service véhic. 
        StructField("description_tournee", IntegerType, true), //Description tournée 
        StructField("km_realise", IntegerType, false), //Km réalisé 
        StructField("no_bon", StringType, true), //noBon filtre pour doublon
        StructField("lot", StringType, true), //LOT 
        StructField("service", IntegerType, false), //Service 
        StructField("nom_rech_lieu_de_vidage", StringType, true), //Nom rech. lieu de vidage 
        StructField("multiples_lignes", DoubleType, true), //Nom multiples ligne 
        StructField("cle_unique_ligne_ticket", FloatType, true) //, //Clé unique Ligne ticket 
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
    ExecuteDechetExutoirePreparation.main(args) match {
    case Right(dfPrepared) => assertEquals(0, dfPrepared.except(dfExpected).count())
    }
  }

}