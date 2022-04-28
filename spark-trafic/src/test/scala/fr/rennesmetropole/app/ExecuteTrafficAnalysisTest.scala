package fr.rennesmetropole.app
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Assert._
import org.junit.Test
import fr.rennesmetropole.app.ExecuteTrafficAnalysis
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
class ExecuteTrafficAnalysisTest {
  @Test
  def testExecuteTrafficAnalysis() : Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val schemaExpected = StructType(
      List(
        StructField("start_time", StringType, false),
        StructField("id", StringType, true),
        StructField("speed_min", StringType, false),
        StructField("speed_avg", StringType, false),
        StructField("speed_max", StringType, false),
        StructField("speed_std", StringType, false),
        StructField("rel_avg", StringType, false),
        StructField("rel_std", StringType, false),
        StructField("nb_values", StringType, false),
        StructField("year", StringType, false),
        StructField("month", StringType, false),
        StructField("week", StringType, false),
        StructField("technical_key", StringType, false)
      )
    )
    val dfExpected = spark
      .read
      .option("header","true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaExpected) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/ExecuteTrafficAnalysis/output/sampleReadOutput.csv")
    val args = Array("2021-01-01","15")
    ExecuteTrafficAnalysis.main(args) match {
      case Right(dfAnalysed) => assertEquals(dfAnalysed.except(dfExpected).count(), 0)
    }

  }

 @Test
  def testExecuteTrafficAnalysisWrongDate() : Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val args = Array("2021-01-39","15")
    ExecuteTrafficAnalysis.main(args) match {
      case Right(dfAnalysed) => assertEquals(dfAnalysed.count(), 0)
    }

  }

}
