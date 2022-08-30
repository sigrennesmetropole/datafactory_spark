package fr.rennesmetropole.services

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import fr.rennesmetropole.tools.Utils

class ImportDechetTest {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkTests")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")


  @Test
  def readFromMinioTest(): Unit = {
    var DATE = "2022-01-01"
    var dfInput = ImportDechet.ExecuteImportDechet(spark, DATE,"tableCollecte")

    val schemaExpected = StructType(
      
      List(
        StructField("Date_de_la_levee", StringType, false),
        StructField("Heure_de_la_levee", StringType, false),
        StructField("Code_puce", StringType, true),
        StructField("Pesee_net", DoubleType, true),
        StructField("Statut_de_la_levee", IntegerType, false),
        StructField("Latitude", DoubleType, true),
        StructField("Longitude", DoubleType, true),
        StructField("Bouton_poussoir_1", IntegerType, true),
        StructField("Bouton_poussoir_2", IntegerType, true),
        StructField("Bouton_poussoir_3", IntegerType, true),
        StructField("Bouton_poussoir_4", IntegerType, true),
        StructField("Bouton_poussoir_5", IntegerType, true),
        StructField("Bouton_poussoir_6", IntegerType, true),
        StructField("Statut_du_bac", StringType, true),
        StructField("Immatriculation", StringType, true),
        StructField("Tournee", StringType, true)
      )
    )

    var dfExpected =  spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaExpected) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/ReadFromMinioTestExpected.csv")
      dfInput.show(false)
      dfExpected.show(false)
      
    assertEquals(0,dfInput.except(dfExpected).count())
  }

  @Test
  def ExecuteImportDechetTest(): Unit = {
    var DATE = "2022-01-01"
    var dfInput = ImportDechet.ExecuteImportDechet(spark, DATE,"tableCollecte")

    val schemaExpected = StructType(
      
      List(
        StructField("Date_de_la_levee", StringType, false),
        StructField("Heure_de_la_levee", StringType, false),
        StructField("Code_puce", StringType, true),
        StructField("Pesee_net", DoubleType, true),
        StructField("Statut_de_la_levee", IntegerType, false),
        StructField("Latitude", DoubleType, true),
        StructField("Longitude", DoubleType, true),
        StructField("Bouton_poussoir_1", IntegerType, true),
        StructField("Bouton_poussoir_2", IntegerType, true),
        StructField("Bouton_poussoir_3", IntegerType, true),
        StructField("Bouton_poussoir_4", IntegerType, true),
        StructField("Bouton_poussoir_5", IntegerType, true),
        StructField("Bouton_poussoir_6", IntegerType, true),
        StructField("Statut_du_bac", StringType, true),
        StructField("Immatriculation", StringType, true),
        StructField("Tournee", StringType, true)
      )
    )

    var dfExpected =  spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaExpected) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/ExecuteImportDechetTestExpected.csv")

    assertEquals(0,dfInput.except(dfExpected).count())

  }
}