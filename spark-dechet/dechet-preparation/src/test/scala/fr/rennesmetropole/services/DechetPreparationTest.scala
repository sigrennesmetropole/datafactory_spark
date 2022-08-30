package fr.rennesmetropole.services

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import fr.rennesmetropole.tools.Utils

class DechetPreparationTest {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkTests")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")


  @Test
  def ExecuteDechetPreparationTest(): Unit = {
    val schema = StructType(
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
    var dfInput =  spark
        .read
        .option("header", "true")
        .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
        .schema(schema) // mandatory
        .option("delimiter", ";")
        .load("src/test/resources/Local/services/preparation/year=2022/month=01/day=01/DechetPreparationInput.csv")

    var dfExpected =  spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schema) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/preparation/DechetPreparationOutput.csv")

    val dfOutput = DechetPreparation.ExecuteDechetPreparation(spark,dfInput,"tableCollecte")

    println("========== TEST: En entree ============")
    dfInput.show()
    
    println("===========TEST: Attendu============")
    dfExpected.show()
    
    println("==========TEST: En sortie============")
    dfOutput.show()

    println("==========Differentiel============")
    dfOutput.except(dfExpected).show()

    println("==========Resultat============")
    val testval = dfOutput.except(dfExpected).count()
    println("Nombre de lignes differentes:" + testval)


    assertEquals(0, dfOutput.except(dfExpected).count())
  }

}