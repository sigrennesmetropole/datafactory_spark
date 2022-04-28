package fr.rennesmetropole.services

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

class DechetRefPreparationTest {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkTests")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  @Test
  def ExecuteDechetRefProdPreparationTest(): Unit = {
    val dfInputProd = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/referentiel/DechetRefProdPreparationInput.csv")

    val dfExpectedProd = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/referentiel/DechetRefProdPreparationOutput.csv")

    val dfOutputProd = DechetRefPreparation.ExecuteDechetRefPreparation(spark,dfInputProd,"tableProducteur")
    
    println("========== TEST PROD: En entree ============")
    dfInputProd.show()
    
    println("===========TEST PROD: Attendu============")
    dfExpectedProd.show()
    
    println("==========TEST PROD: En sortie============")
    dfOutputProd.show()

    println("==========Differentiel============")
    dfOutputProd.except(dfExpectedProd).show()

    println("==========Resultat Producteur============")
    val testval = dfOutputProd.except(dfExpectedProd).count()
    println("Nombre de lignes differentes:" + testval)

    assertEquals(0, dfOutputProd.except(dfExpectedProd).count())
  }

  @Test
  def ExecuteDechetRefBacPreparationTest(): Unit = {
    val dfInputBac = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/referentiel/DechetRefBacPreparationInput.csv")

    val dfExpectedBac = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/referentiel/DechetRefBacPreparationOutput.csv")

    val dfOutputBac = DechetRefPreparation.ExecuteDechetRefPreparation(spark,dfInputBac,"tableRecipient")
    
    println("========== TEST BAC: En entree ============")
    dfInputBac.show()
    
    println("===========TEST BAC: Attendu============")
    dfExpectedBac.show()
    
    println("==========TEST BAC: En sortie============")
    dfOutputBac.show()

    println("==========Differentiel============")
    dfOutputBac.except(dfExpectedBac).show()

    println("==========Resultat Bacs============")
    val testval = dfOutputBac.except(dfExpectedBac).count()
    println("Nombre de lignes differentes:" + testval) 

    assertEquals(0, dfOutputBac.except(dfExpectedBac).count())
  }
}