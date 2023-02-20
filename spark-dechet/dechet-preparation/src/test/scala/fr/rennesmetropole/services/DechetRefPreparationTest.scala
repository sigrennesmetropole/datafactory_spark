package fr.rennesmetropole.services

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import fr.rennesmetropole.tools.Utils
import com.typesafe.config.ConfigFactory

class DechetRefPreparationTest {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkTests")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  val config = ConfigFactory.load()
  val DEBUG = Utils.envVar("TEST_DEBUG")

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
    if(DEBUG =="true"){
        println("========== TEST PROD: En entree ============")
        dfInputProd.show(false)
        
        println("===========TEST PROD: Attendu============")
        dfExpectedProd.show(false)
        
        println("==========TEST PROD: En sortie============")
        dfOutputProd.show(false)

        println("==========Differentiel============")
        println("dfOutputProd.except(dfExpectedProd)")
        dfOutputProd.except(dfExpectedProd).show(false)
        println("dfOutputProd.except(dfExpectedProd)")
        dfOutputProd.except(dfExpectedProd).show(false)

        println("==========Resultat Producteur============")
        var testval = dfExpectedProd.except(dfOutputProd).count()
        println("Nombre de lignes differentes:" + testval)
        testval = dfOutputProd.except(dfExpectedProd).count()
        println("Nombre de lignes differentes:" + testval)
    }
    

    assertEquals(0, dfExpectedProd.except(dfOutputProd).count())
    assertEquals(0, dfOutputProd.except(dfExpectedProd).count())
  }

  @Test
  def ExecuteDechetRefProdPreparationTest_more_column(): Unit = {

    val dfInputProd = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/referentiel/DechetRefProdPreparationInput_more_column.csv")

    val dfExpectedProd = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/referentiel/DechetRefProdPreparationOutput.csv")

    val dfOutputProd = DechetRefPreparation.ExecuteDechetRefPreparation(spark, dfInputProd, "tableProducteur")
    if (DEBUG == "true") {
      println("========== TEST PROD: En entree ============")
      dfInputProd.show(false)

      println("===========TEST PROD: Attendu============")
      dfExpectedProd.show(false)

      println("==========TEST PROD: En sortie============")
      dfOutputProd.show(false)

      println("==========Differentiel============")
      println("dfOutputProd.except(dfExpectedProd)")
      dfOutputProd.except(dfExpectedProd).show(false)
      println("dfOutputProd.except(dfExpectedProd)")
      dfOutputProd.except(dfExpectedProd).show(false)

      println("==========Resultat Producteur============")
      var testval = dfExpectedProd.except(dfOutputProd).count()
      println("Nombre de lignes differentes:" + testval)
      testval = dfOutputProd.except(dfExpectedProd).count()
      println("Nombre de lignes differentes:" + testval)
    }


    assertEquals(0, dfExpectedProd.except(dfOutputProd).count())
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
    
    if(DEBUG =="true"){
        println("========== TEST BAC: En entree ============")
        dfInputBac.show(false)
        
        println("===========TEST BAC: Attendu============")
        dfExpectedBac.show(false)
        
        println("==========TEST BAC: En sortie============")
        dfOutputBac.show(false)

        println("==========Differentiel============")
        println("dfExpectedBac.except(dfOutputBac)")
        dfExpectedBac.except(dfOutputBac).show(false)
        println("dfOutputBac.except(dfExpectedBac)")
        dfOutputBac.except(dfExpectedBac).show(false)

        println("==========Resultat Bacs============")
        var testval = dfExpectedBac.except(dfOutputBac).count()
        println("Nombre de lignes differentes:" + testval) 
        testval = dfOutputBac.except(dfExpectedBac).count()
        println("Nombre de lignes differentes:" + testval) 
    }

    assertEquals(0, dfExpectedBac.except(dfOutputBac).count())
    assertEquals(0, dfOutputBac.except(dfExpectedBac).count())
  }
}