package fr.rennesmetropole.app

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import fr.rennesmetropole.tools.Utils

class ExecuteDechetPreparationTest {
  val config = ConfigFactory.load()
  val DEBUG = Utils.envVar("TEST_DEBUG")
  @Test
  def ExecuteDechetPreparationTest(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()

    val schemaOutput = StructType(
      List(
        StructField("Date_de_la_levee", StringType, false),
        StructField("Heure_de_la_levee", StringType, false),
        StructField("Code_puce", StringType, true),
        StructField("Pesee_net", DoubleType, true),
        StructField("Statut_de_la_levee", IntegerType, false),
        StructField("Latitude", DoubleType, true),
        StructField("Longitude", DoubleType, true),
        StructField("Bouton_poussoir_1", DoubleType, true),
        StructField("Bouton_poussoir_2", DoubleType, true),
        StructField("Bouton_poussoir_3", DoubleType, true),
        StructField("Bouton_poussoir_4", DoubleType, true),
        StructField("Bouton_poussoir_5", DoubleType, true),
        StructField("Bouton_poussoir_6", DoubleType, true),
        StructField("Statut_du_bac", StringType, true),
        StructField("Immatriculation", StringType, true),
        StructField("Tournee", StringType, true)
      )
    )


    val dfExpected = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutput) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetPreparation/Output/ExecuteDechetPreparationTestOutput.csv")

    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-01-01")
    ExecuteDechetPreparation.main(args) match {
    case Right(dfPrepared) => {
        if(DEBUG == "true"){
            println(" =======  DATAFRAME PREPARED df_bac  =======  ") 
            dfPrepared.show(false)
            println(" =======  DATAFRAME EXPECTED df_bac  =======  ") 
            dfExpected.show(false)
        }
        assertEquals(0, dfPrepared.except(dfExpected).count())
        assertEquals(0, dfExpected.except(dfPrepared).count())
    }
  }
  }

}