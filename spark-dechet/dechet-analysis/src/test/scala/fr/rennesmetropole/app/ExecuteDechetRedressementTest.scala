package fr.rennesmetropole.app

import fr.rennesmetropole.tools.Utils
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.junit.Test

import java.io.File


class ExecuteDechetRedressementTest {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkTests")
    .getOrCreate()
  val DEBUG = Utils.envVar("TEST_DEBUG")

  //executer ce code si le referentiel bac au format csv a été changé puis renommer le nouveau orc obtenue en "ref_bac" et supprimer le superflus
  @Test
  def prerequis_test():Unit = {
    spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .csv("src/test/resources/Local/app/ExecuteDechetRedressement/referentiel/csv/ReferentielBacDechetRedressementTest.csv")
      .repartition(1)   // nécessaire pour écrire le DF dans un seul csv, mais pPeut poser problème si le DF est trop gros
      .write
      .mode(SaveMode.Append)
      .orc("src/test/resources/Local/app/ExecuteDechetRedressement/referentiel/orc")


  }
  @Test
  def ExecuteDechetRedressementTest(): Unit = {

    println("\n#\n#\n ############################### ExecuteDechetAnalysisTest ############################### \n#\n#")
    //deplacement du referentiel pour le redressement (fait comme ça pour eviter de devoir rajouter trop de paths de test dans le fichier de conf)
    var directory = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_bac/orc/");
    FileUtils.cleanDirectory(directory);
    //mise en place des fichiers referentiel necessaire a l'analyse des donnees de collecte
    val destBac = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_bac/orc/ref_bac.orc")
    val srcBac = new File("src/test/resources/Local/app/ExecuteDechetRedressement/referentiel/orc/ref_bac.orc")
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
      .csv("src/test/resources/Local/app/ExecuteDechetRedressement/Output/year=2022/month=01/day=01/ExecuteDechetRedressementTestOutput.csv")

    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-07-01","2022-07-01T12:00:00.000Z")
      ExecuteDechetRedressement.main(args) match {
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