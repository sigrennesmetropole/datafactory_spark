package fr.rennesmetropole.tools

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._

class UtilsTest {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkTests")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  @Test
  def testEnvVar(): Unit = {
    val endpoint = ConfigFactory.load().getString("env.S3_ENDPOINT")
    assertEquals(Utils.envVar("S3_ENDPOINT"), endpoint)
    assertThrows[Exception] {
      Utils.envVar("NOT_A_VAR")
    }
  }

  @Test
  def testReadData(): Unit = {

    val schema = StructType(
      List(
        StructField("time", StringType, true),
        StructField("error", StringType, false),
        StructField("body", StringType, false)
      )
    )

    var DATE = java.time.LocalDate.now.toString
    var dfInput = Utils.readData(spark, DATE, schema, "0018b21000002f1e")
    var dfExpected = spark.read.json("src/test/resources/Local/Utils/readDataExpected.json")
    assertEquals(dfInput.except(dfExpected).count(), 0)
  }
}