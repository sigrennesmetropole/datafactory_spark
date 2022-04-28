package fr.rennesmetropole.tools

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import fr.rennesmetropole.tools.Traitements

class TraitementsTest {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkTests")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  //TODO

 /* @Test
  def traitement_Adeunis_RF_Lorawan_TempTest(): Unit = {
    var dfInput = spark.read.json("src/test/resources/Local/tools/traitement_Adeunis_RF_Lorawan_TempInput.json")

    val schemaExpected = StructType(
      List(
        StructField("deveui", StringType, true),
        StructField("timestamp", StringType, false),
        StructField("name", StringType, false),
        StructField("value", StringType, false),
        StructField("insertedDate", StringType, false),
        StructField("id", StringType, false)
      )
    )
    val dfExpected = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaExpected) // mandatory
      .option("delimiter", ",")
      .load("src/test/resources/Local/tools/traitement_Adeunis_RF_Lorawan_TempExpected.csv")
    val param = Array(Seq("test"))
    var dfOutput = Traitements.traitement_Adeunis_RF_Lorawan_Temp(dfInput, "0018b21000002f1e", spark, param)
    assertEquals(dfOutput.drop("insertedDate").except(dfExpected.drop("insertedDate")).count(), 0)

  }

  @Test
  def traitement_NKE_TIC_PME_PMITest(): Unit = {
    var dfInput = spark.read.json("src/test/resources/Local/tools/traitement_NKE_TIC_PME_PMIInput.json")

    val schemaExpected = StructType(
      List(
        StructField("deveui", StringType, true),
        StructField("timestamp", StringType, false),
        StructField("name", StringType, false),
        StructField("value", StringType, false),
        StructField("insertedDate", StringType, false),
        StructField("id", StringType, false)
      )
    )

    val dfExpected = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaExpected) // mandatory
      .option("delimiter", ",")
      .load("src/test/resources/Local/tools/traitement_NKE_TIC_PME_PMIIExpected.csv")
    val param = Array(Seq("test"))
    var dfOutput = Traitements.traitement_NKE_TIC_PME_PMI(dfInput, "70b3d5e75e003dcd", spark, param)
    assertEquals(dfOutput.drop("insertedDate").except(dfExpected.drop("insertedDate")).count(), 0)
  }


  // try without EAP_i
  @Test
  def traitement_NKE_TIC_PME_PMI2Test(): Unit = {
    var dfInput = spark.read.json("src/test/resources/Local/tools/traitement_NKE_TIC_PME_PMIInput2.json")

    val schemaExpected = StructType(
      List(
        StructField("deveui", StringType, true),
        StructField("timestamp", StringType, false),
        StructField("name", StringType, false),
        StructField("value", StringType, false),
        StructField("insertedDate", StringType, false),
        StructField("id", StringType, false)
      )
    )

    val dfExpected = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaExpected) // mandatory
      .option("delimiter", ",")
      .load("src/test/resources/Local/tools/traitement_NKE_TIC_PME_PMIIExpected2.csv")
    val param = Array(Seq("test"))
    var dfOutput = Traitements.traitement_NKE_TIC_PME_PMI(dfInput, "70b3d5e75e003dcd", spark, param)
    assertEquals(dfOutput.drop("insertedDate").except(dfExpected.drop("insertedDate")).count(), 0)
  }*/
}