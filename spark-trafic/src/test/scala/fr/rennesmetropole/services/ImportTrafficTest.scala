package fr.rennesmetropole.services

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import fr.rennesmetropole.services.ImportTraffic
import fr.rennesmetropole.tools.Utils

class ImportTrafficTest {
  @Test
  def testExecuteImportTraffic() : Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val schema = StructType(
      List(
        StructField("date", StringType, true),
        StructField("segment", StringType, false),
        StructField("avgSpeed", IntegerType, false),
        StructField("travelTime", IntegerType, false),
        StructField("reliability", IntegerType, false),
        StructField("status", StringType, false)
      )
    )
    val dfExpected = spark
      .read
      .option("header","true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schema) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/ImportTraffic/output/sampleReadOutput.csv")
    val dfImportTrafficOutput = ImportTraffic.ExecuteImportTraffic(spark,"2021-01-01")
    assertEquals(dfExpected.except(dfImportTrafficOutput).count(), 0)
  }
}
