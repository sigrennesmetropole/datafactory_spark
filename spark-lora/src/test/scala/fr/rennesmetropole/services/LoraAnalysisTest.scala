package fr.rennesmetropole.services

import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.junit.Test

class LoraAnalysisTest {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkTests")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  spark.sparkContext
    .hadoopConfiguration
    .set("fs.s3a.access.key", Utils.envVar("S3_ACCESS_KEY"))
  spark.sparkContext
    .hadoopConfiguration
    .set("fs.s3a.secret.key", Utils.envVar("S3_SECRET_KEY"))
  spark.sparkContext
    .hadoopConfiguration
    .set("fs.s3a.endpoint", Utils.envVar("S3_ENDPOINT"))
  spark.sparkContext
    .hadoopConfiguration
    .set("fs.s3a.path.style.access", "true")
  spark.sparkContext
    .hadoopConfiguration
    .set("fs.s3.aws.credentials.provider", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

  @Test
  def ExecuteLoraAnalysisTest(): Unit = {
    var dfInput = spark.read.json("src/test/resources/Local/services/ExecuteLoraAnalysisInput/")
    var dfOutput = LoraAnalysis.ExecuteLoraAnalysis(spark, dfInput,"0018b21000002f1e", null)
      .unionByName(LoraAnalysis.ExecuteLoraAnalysis(spark, dfInput,"70b3d5e75e003dcd", null))

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
      .load("src/test/resources/Local/services/ExecuteLoraAnalysisExpected.csv")

    assertEquals(dfOutput.drop("insertedDate").except(dfExpected.drop("insertedDate")).count(), 0)
  }


  @Test
  def enrichissement_BasiqueTest(): Unit = {

    val schemaInput = StructType(
      List(
        StructField("deveui", StringType, false),
        StructField("timestamp", StringType, false),
        StructField("name", StringType, false),
        StructField("value", StringType, false),
        StructField("insertedDate", StringType, false),
        StructField("id", StringType, false)
      )
    )


    val dfInput = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaInput) // mandatory
      .option("delimiter", ",")
      .load("src/test/resources/Local/services/enrichissement_BasiqueInput.csv")

    val schemaExpected = StructType(
      List(
        StructField("id", StringType, true),
        StructField("deveui", StringType, false),
        StructField("enabledon", StringType, false),
        StructField("timestamp", StringType, false),
        StructField("name", StringType, false),
        StructField("value", StringType, false),
        StructField("inserteddate", StringType, false),
        StructField("measurenatureid", StringType, false),
        StructField("unitid", StringType, false)
      )
    )

    val dfExpected = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaExpected) // mandatory
      .option("delimiter", ",")
      .load("src/test/resources/Local/services/enrichissement_BasiqueExpected.csv")

    var dfOutput = LoraAnalysis.enrichissement_Basique(spark, dfInput)
    assertEquals(dfOutput.except(dfExpected).count(), 0)
  }


}