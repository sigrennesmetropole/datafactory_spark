package fr.rennesmetropole.services

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import fr.rennesmetropole.tools.Utils

class DechetExutoireAnalysisTest {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkTests")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")


  @Test
  def ExecuteDechetAnalysisTest(): Unit = {
    val schemaInput = StructType(
      List(
        StructField("immat", StringType, false), //Immat. 
        StructField("dateServiceVehic", StringType, true), //Date service véhic. 
        StructField("codeTournee", IntegerType, true), //Description tournée 
        StructField("kmRealise", IntegerType, false), //Km réalisé 
        StructField("noBon", StringType, true), //noBon filtre pour doublon
        StructField("lot", StringType, true), //LOT 
        StructField("service", IntegerType, false), //Service 
        StructField("nomRechLieuDeVidage", StringType, true), //Nom rech. lieu de vidage 
        StructField("multiplesLignes", DoubleType, true), //Nom multiples ligne 
        StructField("cleUniqueLigneTicket", FloatType, true) //, //Clé unique Ligne ticket 
        )
    )
    var dfInput =  spark
        .read
        .option("header", "true")
        .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
        .schema(schemaInput) // mandatory
        .option("delimiter", ";")
        .load("src/test/resources/Local/services/Exutoire/DechetExutoireAnalysisInput.csv")

    val schemaOutput = StructType(
      List(
        StructField("code_immat", StringType, false), //Immat. 
        StructField("date_mesure", StringType, true), //Date service véhic. 
        StructField("code_tournee", IntegerType, true), //Description tournée 
        StructField("distance", IntegerType, false), //Km réalisé 
        //StructField("noBon", StringType, true), //noBon filtre pour doublon
        StructField("secteur", StringType, true), //LOT 
        StructField("nb_bac", IntegerType, false), //Service 
        StructField("localisation_vidage", StringType, true), //Nom rech. lieu de vidage 
        StructField("poids", DoubleType, true), //Nom multiples ligne 
        StructField("type_vehicule", FloatType, true), //, //Clé unique Ligne ticket 
        StructField("date_crea", StringType, false),
        StructField("date_modif", StringType, false) ,
        StructField("year", StringType, false),
        StructField("month", StringType, false),
        StructField("day", StringType, false) 
        )
    )

    var dfExpected =  spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutput) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Exutoire/DechetExutoireAnalysisOutput.csv")
    val dfOutput = DechetExutoireAnalysis.ExecuteDechetExutoireAnalysis(spark,dfInput,"2021-01-01")
    assertEquals(0, dfOutput.except(dfExpected).count())
  }

 /*@Test
  def mergeToTimestampTest(): Unit = {
        assertEquals("2021-10-27 05:58:38+01",DechetAnalysis.mergeToTimestamp("20211027","055838"))
        assertEquals("2020-05-08 15:00:25+01",DechetAnalysis.mergeToTimestamp("20200508","150025"))
  }*/
}