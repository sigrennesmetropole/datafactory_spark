package fr.rennesmetropole.services

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import fr.rennesmetropole.services.AnalyseTraffic
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory

class AnalyseTrafficTest {
  @Test
  def testExecuteAnalyseTraffic() : Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val schemaExpected = StructType(
      List(
        StructField("id", StringType, true),
        StructField("start_time", StringType, false),
        StructField("speed_min", StringType, false),
        StructField("speed_avg", StringType, false),
        StructField("speed_max", StringType, false),
        StructField("speed_std", StringType, false),
        StructField("rel_avg", StringType, false),
        StructField("rel_std", StringType, false),
        StructField("nb_value", StringType, false)
      )
    )
    val dfExpected = spark
      .read
      .option("header","true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaExpected) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/AnalyseTraffic/output/sampleReadOutput.csv")

    val schemaInput = StructType(
      List(
        StructField("date", StringType, true),
        StructField("segment", StringType, false),
        StructField("avgSpeed", IntegerType, false),
        StructField("travelTime", IntegerType, false),
        StructField("reliability", IntegerType, false),
        StructField("status", StringType, false)
      )
    )
    val dfInput = spark
      .read
      .option("header","true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaInput) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/AnalyseTraffic/input/sampleReadInput.csv")

    val dfAnalyseTrafficOutput = AnalyseTraffic.ExecuteAnalyseTraffic(spark, dfInput, 15)
    assertEquals(dfExpected.except(dfAnalyseTrafficOutput).count(), 0)
  }
}
