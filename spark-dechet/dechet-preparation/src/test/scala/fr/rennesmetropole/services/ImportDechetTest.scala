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


 /* @Test
  def readFromMinioTest(): Unit = {
    var DATE = java.time.LocalDate.now.toString
    var dfInput = ImportDechet.ExecuteImportDechet(spark, DATE)

    val schemaExpected = StructType(
      
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
        StructField("Bouton_poussoir_6", DoubleType, true)
        //StructField("Statut bac", StringType, true),
        //StructField("Tournee", StringType, true),
        //StructField("Immatriculation ", StringType, true)
      )
    )

    var dfExpected =  spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaExpected) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/ReadFromMinioTestExpected.csv")
    assertEquals(dfInput.except(dfExpected).count(), 0)
  }

  @Test
  def ExecuteImportDechetTest(): Unit = {
    var DATE = java.time.LocalDate.now.toString
    var dfInput = ImportDechet.ExecuteImportDechet(spark, DATE)

    val schemaExpected = StructType(
      
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
        StructField("Bouton_poussoir_6", DoubleType, true)
        //StructField("Statut bac", StringType, true),
        //StructField("Tournee", StringType, true),
        //StructField("Immatriculation ", StringType, true)
      )
    )

    var dfExpected =  spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaExpected) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/ExecuteImportDecheTestExpected.csv")

    assertEquals(dfInput.except(dfExpected).count(), 0)

  }*/
}