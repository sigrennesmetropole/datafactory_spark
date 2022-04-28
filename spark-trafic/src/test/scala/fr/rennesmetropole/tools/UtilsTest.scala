package fr.rennesmetropole.tools

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.util.{Calendar, Date}

class TestUtils {
    @Test
    def testEnvVar() : Unit = {
        val endpoint = ConfigFactory.load().getString("env.S3_ENDPOINT");
                assertEquals(Utils.envVar("S3_ENDPOINT"), endpoint);
                assertThrows[Exception] {
                    Utils.envVar("NOT_A_VAR");
                }
    }

  @Test
  def testReadData () : Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val schema = StructType(
      List(
        StructField("datetime", StringType, true),
        StructField("predefinedLocationReference", StringType, false),
        StructField("averageVehicleSpeed", IntegerType, false),
        StructField("travelTime", IntegerType, false),
        StructField("travelTimeReliability", IntegerType, false),
        StructField("trafficStatus", StringType, false)
      )
    )
    val dfInput = Utils.readData(spark, schema,"2021-01-01")
    val dfOutput = spark
      .read
      .option("header","true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schema) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/Utils/output/sampleReadOutput.csv")
    val result = dfOutput.except(dfInput)
    assertEquals(result.count,0)
  }
  @Test
  def testDate2URL() : Unit = {
    assertEquals(Utils.date2URL("2021-01-01"), "year=2021/month=01/week=01/")
    assertEquals(Utils.date2URL("2021-10-10"), "year=2021/month=10/week=02/")
    assertEquals(Utils.date2URL("2021-10-19"), "year=2021/month=10/week=03/")
    assertEquals(Utils.date2URL("2021-10-25"), "year=2021/month=10/week=04/")
    assertEquals(Utils.date2URL("2021-10-29"), "year=2021/month=10/week=05/")
    assertEquals(Utils.date2URL("2021-10-39"), "year=2021/month=10/week=-1/")
  }

  @Test
  def testDay2week() : Unit = {
    assertEquals(Utils.day2Week(1), "01")
    assertEquals(Utils.day2Week(7), "01")
    assertEquals(Utils.day2Week(8), "02")
    assertEquals(Utils.day2Week(14), "02")
    assertEquals(Utils.day2Week(15), "03")
    assertEquals(Utils.day2Week(21), "03")
    assertEquals(Utils.day2Week(22), "04")
    assertEquals(Utils.day2Week(28), "04")
    assertEquals(Utils.day2Week(29), "05")
    assertEquals(Utils.day2Week(36), "-1")

  }

  @Test
  def testFormatData() : Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val schema = StructType(
      List(
        StructField("datetime", StringType, true),
        StructField("predefinedLocationReference", StringType, false),
        StructField("averageVehicleSpeed", IntegerType, false),
        StructField("travelTime", IntegerType, false),
        StructField("travelTimeReliability", IntegerType, false),
        StructField("trafficStatus", StringType, false)
      )
    )

    val formatColumns = Map("dateTime" -> "date",
      "predefinedLocationReference" -> "segment",
      "averageVehicleSpeed" -> "avgSpeed",
      "travelTime" -> "travelTime",
      "travelTimeReliability" -> "reliability",
      "trafficStatus" -> "status")

    val dfInput = spark
      .read
      .option("header","true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schema) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/Utils/input/sampleFormatDataFunctionInput.csv")

    val dfOutput = spark
      .read
      .option("header","true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schema) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/Utils/output/sampleFormatDataFunctionOutput.csv")
    val dfFormatted = Utils.formatData(dfInput, formatColumns)
    assertEquals(dfFormatted.except(dfOutput).count(), 0)

  }

  @Test
  def testFormatColumnDate() : Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val dfInput = spark
      .read
      .option("header","true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/Utils/input/sampleFormatColumnDateFunctionInput.csv")

    val dfOutput = spark
      .read
      .option("header","true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/Utils/output/sampleFormatColumnDateFunctionOutput.csv")
    val dfProcessed = Utils.formatColumnDate(dfInput, "date", "yyyy-MM-dd'T'HH:mm:ss")
    dfProcessed.show(20, false)
    dfOutput.show(20,false)
    assertEquals(dfProcessed.except(dfOutput).count(), 0)
  }

  @Test
  def testParseDateISO() : Unit = {
    assertEquals(Utils.parseDateISO("2021-02-25T15:22:00+02:00"), "2021-02-25 15:22:00")
  }

  @Test
  def filterDataframeBetweenTwoHours() : Unit = {
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
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val dfInput = spark
      .read
      .option("header","true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schema) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/Utils/input/sampleFilterDateBetweenHoursInput.csv")

    val dfExpected = spark
      .read
      .option("header","true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schema) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/Utils/output/sampleFilterDateBetweenHoursOutput.csv")

    val dfOutput = Utils.filterDataframeBetweenTwoHours(dfInput,4,22)
    assertEquals(dfOutput.except(dfExpected).count(),0)
  }

  @Test
  def testRoundDateToLatestQuarter() : Unit = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateInput1 = format.parse("2021-03-27 15:52:02")
    val dateExpected1 = format.parse("2021-03-27 15:45:00")
    val dateInput2 = format.parse("2021-03-27 15:12:59")
    val dateExpected2 = format.parse("2021-03-27 15:00:00")
    val dateInput3 = format.parse("2021-03-27 18:29:59")
    val dateExpected3 = format.parse("2021-03-27 18:15:00")
    val dateInput4 = format.parse("2021-03-27 11:30:01")
    val dateExpected4 = format.parse("2021-03-27 11:30:00")
    assertEquals(Utils.roundDateToLatestQuarter(dateInput1), dateExpected1)
    assertEquals(Utils.roundDateToLatestQuarter(dateInput2), dateExpected2)
    assertEquals(Utils.roundDateToLatestQuarter(dateInput3), dateExpected3)
    assertEquals(Utils.roundDateToLatestQuarter(dateInput4), dateExpected4)
  }
}