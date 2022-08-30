package fr.rennesmetropole.app

import fr.rennesmetropole.tools.Utils
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.junit.Test

import java.io.File


class ExecuteDechetAnalysisTest {

  @Test
  def ExecuteDechetAnalysisTest(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkTests")
      .getOrCreate()
    val DEBUG = Utils.envVar("TEST_DEBUG")
    println("\n#\n#\n ############################### ExecuteDechetAnalysisTest ############################### \n#\n#")
    var directory = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_prod/orc/");
    FileUtils.cleanDirectory(directory);
    directory = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_bac/orc/");
    FileUtils.cleanDirectory(directory);
    //mise en place des fichiers referentiel necessaire a l'analyse des donnees de collecte
    val destProd = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_prod/orc/ref_prod.orc")
    val destBac = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_bac/orc/ref_bac.orc")
    val srcProd = new File("src/test/resources/Local/app/ExecuteDechetAnalysis/referentiel/ref_prod.orc")
    val srcBac = new File("src/test/resources/Local/app/ExecuteDechetAnalysis/referentiel/ref_bac.orc")
    FileUtils.copyFile(srcProd,destProd)
    FileUtils.copyFile(srcBac,destBac)

    val schemaOutput = StructType(
      List(
        StructField("date_mesure", StringType, true),
        StructField("code_puce", StringType, true),
        StructField("id_bac", StringType, false),
        StructField("code_tournee", StringType, true),
        StructField("code_immat", StringType, true),
        StructField("poids", StringType, true),
        StructField("poids_corr", StringType, true),
        StructField("latitude", StringType, true),
        StructField("longitude", StringType, true),
        StructField("date_crea", StringType, true),
        StructField("date_modif", StringType, true),
        StructField("type_flux", StringType, true),
        StructField("year", StringType, true),
        StructField("month", StringType, true),
        StructField("day", StringType, true)
      )
    )

    val dfExpected = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutput) // mandatory
      .option("delimiter", ";")
      .csv("src/test/resources/Local/app/ExecuteDechetAnalysis/Output/year=2022/month=01/day=01/ExecuteDechetAnalysisTestOutput.csv")

    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-01-01","2022-01-01T12:00:00.00Z")
    ExecuteDechetAnalysis.main(args) match {
    case Right(df_collecte) => {
      if(DEBUG == "true"){

        println(" =======  SCHEMA df_bac  ======= ")
        df_collecte.printSchema()
        dfExpected.printSchema()

        println(" =======  DATAFRAME ANALISE df_collecte =======  ")
        df_collecte.show(false)
        println(" =======  DATAFRAME EXPECTED df_collecte  =======  ")
        dfExpected.show(false)
        println(" =======  DATAFRAME  DIFF df_collecte   =======  ")
        df_collecte.except(dfExpected).show(false)
        dfExpected.except(df_collecte).show(false)
      }

      // Assert ....
      print(" TEST df_collecte  ...  ")
      assertEquals(0, dfExpected.except(df_collecte).count())
      assertEquals(0, df_collecte.except(dfExpected).count())
      println("OK")
    }
  }
  }

}