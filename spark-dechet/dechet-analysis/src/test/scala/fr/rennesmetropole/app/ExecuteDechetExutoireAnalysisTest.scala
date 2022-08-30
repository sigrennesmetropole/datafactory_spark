package fr.rennesmetropole.app

import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.junit.Test

class ExecuteDechetExutoireAnalysisTest {

  @Test
  def ExecuteDechetExutoireAnalysisTest(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()
    val DEBUG = Utils.envVar("TEST_DEBUG")

    val schemaOutput = StructType(
      List(
        StructField("code_immat", StringType, true), //Immat.
        StructField("date_mesure", TimestampType, true), //Date service véhic.
        StructField("code_tournee", StringType, true), //Description tournée
        StructField("distance", DoubleType, true), //Km réalisé
        StructField("secteur", StringType, true), //LOT 
        StructField("nb_bac", IntegerType, true), //Service
        StructField("localisation_vidage", StringType, true), //Nom rech. lieu de vidage 
        StructField("poids", DoubleType, true), //Nom multiples ligne
        StructField("type_vehicule", StringType, true), //Clé unique Ligne ticket
        StructField("date_crea", TimestampType, true),
        StructField("date_modif", TimestampType, true),
        StructField("year", StringType, true),
        StructField("month", StringType, true),
        StructField("day", StringType, true)
        )
    )


    val dfExpected = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutput) // mandatory
      .option("delimiter", ";")
      .csv("src/test/resources/Local/app/ExecuteDechetExutoireAnalysis/Output/year=2022/month=01/day=01/ExecuteDechetExutoireAnalysisTestOutput.csv")

    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-01-01","2022-01-01T12:00:00.00Z")
    ExecuteDechetExutoireAnalysis.main(args) match {
    case Right(df_exutoire) => {

      if(DEBUG == "true"){
          println(" =======  SCHEMA df_exutoire  =======  ")
          df_exutoire.printSchema()
          println(" =======  DATAFRAME ANALYSED df_exutoire  =======  ")
          df_exutoire.show(false)
          println(" =======  DATAFRAME EXPECTED df_exutoire  =======  ")
          dfExpected.show(false)
          println(" =======  DATAFRAME DIFF df_exutoire   =======  ")
           println("ligne que je ne devrais pas avoir mais que j'ai  :")
          df_exutoire.except(dfExpected).show(false)
          println("ligne que je devrais avoir mais que je n'ai pas :")
          dfExpected.except(df_exutoire).show(false)
        }
        // Assert ....
        print(" TEST df_exutoire  ...  ")
        assertEquals(0, dfExpected.except(df_exutoire).count())
        assertEquals(0, df_exutoire.except(dfExpected).count())
        println("OK")
      }
    }
  }

}