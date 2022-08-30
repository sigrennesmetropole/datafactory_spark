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

    val schemaInput = StructType(
      List(
        StructField("Date_de_la_levee", StringType),
        StructField("Heure_de_la_levee", StringType),
        StructField("Code_puce", StringType),
        StructField("Pesee_net", StringType),
        StructField("Statut_de_la_levee", StringType),
        StructField("Latitude", StringType),
        StructField("Longitude", StringType),
        StructField("Bouton_poussoir_1", StringType),
        StructField("Bouton_poussoir_2", StringType),
        StructField("Bouton_poussoir_3", StringType),
        StructField("Bouton_poussoir_4", StringType),
        StructField("Bouton_poussoir_5", StringType),
        StructField("Bouton_poussoir_6", StringType)
        //StructField("Statut bac", StringType),
        //StructField("Tournee", StringType),
        //StructField("Immatriculation ", StringType)
      )
    )

  val DATE = "2022-01-01"
    var dfInput = Utils.readData(spark, DATE, schemaInput, "env")
    var dfExpected = 
      spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaInput) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/utils/year=2022/month=01/day=01/UtilsReadInput.csv")
      dfInput.show(false)
      dfExpected.show(false)
    assertEquals(0, dfInput.except(dfExpected).count())
  }

   @Test
  def testReadDataWithWrongDate(): Unit = {

    val schemaInput = StructType(
      List(
        StructField("Date_de_la_levee", StringType),
        StructField("Heure_de_la_levee", StringType),
        StructField("Code_puce", StringType),
        StructField("Pesee_net", StringType),
        StructField("Statut_de_la_levee", StringType),
        StructField("Latitude", StringType),
        StructField("Longitude", StringType),
        StructField("Bouton_poussoir_1", StringType),
        StructField("Bouton_poussoir_2", StringType),
        StructField("Bouton_poussoir_3", StringType),
        StructField("Bouton_poussoir_4", StringType),
        StructField("Bouton_poussoir_5", StringType),
        StructField("Bouton_poussoir_6", StringType)
        //StructField("Statut bac", StringType),
        //StructField("Tournee", StringType),
        //StructField("Immatriculation ", StringType)
      )
    )

  val DATE = "WrongDate"
  var dfInput = Utils.readData(spark, DATE, schemaInput, "env")
    assertEquals(0, dfInput.count())
  }

  @Test
  def testDate2URL() : Unit = {
    assertEquals(Utils.date2URL("2021-01-01"), "year=2021/month=01/day=01/")
    assertEquals(Utils.date2URL("2021-10-10"), "year=2021/month=10/day=10/")
    assertEquals(Utils.date2URL("2020-10-19"), "year=2020/month=10/day=19/")
    assertEquals(Utils.date2URL("2021-05-25"), "year=2021/month=05/day=25/")
    assertEquals(Utils.date2URL("2021-10-39"), "year=2021/month=10/day=39/")
  }
}