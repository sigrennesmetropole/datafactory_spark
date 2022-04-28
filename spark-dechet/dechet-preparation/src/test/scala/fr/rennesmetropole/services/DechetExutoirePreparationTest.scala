package fr.rennesmetropole.services

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

class DechetExutoirePreparationTest {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkTests")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  @Test
  def ExecuteDechetExutoirePreparationTest(): Unit = {
    val dfInput = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/exutoire/DechetExutoirePreparationInput.csv")

    val dfExpected = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/exutoire/DechetExutoirePreparationOutput.csv")

    val dfOutput = DechetExutoirePreparation.ExecuteDechetExutoirePreparation(spark,dfInput,"tableExutoire")
    
/*     println("========== TEST: En entree ============")
    dfInput.show()
    
    println("===========TEST: Attendu============")
    dfExpected.show()
    
    println("==========TEST: En sortie============")
    dfOutput.show()

    println("==========Differentiel============")
    dfOutput.except(dfExpected).show()

    println("==========Resultat============")
    val testval = dfOutput.except(dfExpected).count()
    println("Nombre de lignes differentes:" + testval) */

    assertEquals(0, dfOutput.except(dfExpected).count())
  }

}