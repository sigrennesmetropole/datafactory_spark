package fr.rennesmetropole.services

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

class ImportTrameLoraTest {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkTests")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")


  @Test
  def readFromMinioTest(): Unit = {
    var DATE = java.time.LocalDate.now.toString
    var dfInput = ImportTrameLora.readFromMinio(spark, DATE, "0018b21000002f1e")
      .unionByName(ImportTrameLora.readFromMinio(spark, DATE, "70b3d5e75e003dcd"))
    var dfExpected = spark.read.json("src/test/resources/Local/services/readFromMinioExpected.json")
    assertEquals(dfInput.except(dfInput).count(), 0)
  }

  @Test
  def ExecuteImportTrameLoraTest(): Unit = {
    var DATE = java.time.LocalDate.now.toString
    var dfInput = ImportTrameLora.ExecuteImportLora(spark, DATE,"0018b21000002f1e")
    var dfExpected = spark.read.json("src/test/resources/Local/services/ExecuteImportLoraExpected.json")
    assertEquals(dfInput.except(dfInput).count(), 0)

  }
}