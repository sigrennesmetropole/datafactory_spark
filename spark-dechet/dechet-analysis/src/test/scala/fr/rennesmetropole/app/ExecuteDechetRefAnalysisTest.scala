package fr.rennesmetropole.app

import com.typesafe.config.ConfigFactory
import fr.rennesmetropole.tools.Utils
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._

import java.io.File;

class ExecuteDechetRefAnalysisTest {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkTests")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  val config = ConfigFactory.load()
  val DEBUG = Utils.envVar("TEST_DEBUG")

  @Test
  def ExecuteDechetRefAnalysisTest_solo(): Unit = {
    println("\n#\n#\n ############################### ExecuteDechetRefAnalysisTest ############################### \n#\n#")
    
    var directory = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_prod/orc/");
    FileUtils.cleanDirectory(directory);
    directory = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_bac/orc/");
    FileUtils.cleanDirectory(directory);

    val schemaOutputProd = StructType(
        List(
          StructField("id_producteur", LongType, false),
          StructField("code_producteur", StringType, true),
          StructField("id_rva", StringType, true),
          StructField("commune", StringType, true),
          StructField("code_insee", StringType, true),
          StructField("type_producteur", StringType, true),
          StructField("activite", StringType, true),
          StructField("latitude", StringType, true),
          StructField("longitude", StringType, true),
          StructField("date_debut", TimestampType, true),
          StructField("date_fin", TimestampType, true),
          StructField("date_crea", TimestampType, true),
          StructField("date_modif", TimestampType, true),
          StructField("year", StringType, true),
          StructField("month", StringType, true),
          StructField("day", StringType, true)
        )
      )
    val schemaOutputBac = StructType(
        List(
          StructField("id_bac", LongType, false),
          StructField("code_puce", StringType, true),
          StructField("code_producteur", StringType, true),
          StructField("categorie_recipient", StringType, true),
          StructField("type_recipient", StringType, true),
          StructField("litrage_recipient", StringType, true),
          StructField("type_puce", StringType, true),
          StructField("nb_collecte", DoubleType, true),
          StructField("date_debut", TimestampType, true),
          StructField("date_fin", TimestampType, true),
          StructField("date_crea", TimestampType, true),
          StructField("date_modif", TimestampType, true),
          StructField("year", StringType, true),
          StructField("month", StringType, true),
          StructField("day", StringType, true),
          StructField("id_producteur", LongType, false)
        )
      )
    println("LECTURE EXPECTED")
    var dfExpectedBac = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputBac) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Output/bac/year=2022/month=01/day=01/DechetRefAnalysisTestOutputBac.csv")
dfExpectedBac.show()
    var dfExpectedProd = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputProd) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Output/prod/year=2022/month=01/day=01/DechetRefAnalysisTestOutputProd.csv")
    val args = Array("2022-01-01")
    println("call")
    ExecuteDechetRefAnalysis.main(args) match {
      case Right(df) =>{
        var df_bac = df._1
        var df_prod = df._2
        //LOG DEBUG
        dfExpectedBac = dfExpectedBac.drop("date_crea","date_modif")
        df_bac = df_bac.drop("date_crea","date_modif")
        dfExpectedProd = dfExpectedProd.drop("date_crea","date_modif")
        df_prod = df_prod.drop("date_crea","date_modif")
        if(DEBUG == "true"){
          
          println(" =======  SCHEMA df_bac  =======  ")
          df_bac.printSchema()
          println(" =======  SCHEMA df_prod  =======  ") 
          df_prod.printSchema()
          println(" =======  DATAFRAME ANALISE df_bac  =======  ") 
          df_bac.show(false)
          println(" =======  DATAFRAME EXPECTED df_bac  =======  ") 
          dfExpectedBac.show(false)
          println(" =======  DATAFRAME DIFF df_bac   =======  ") 
          df_bac.except(dfExpectedBac).show(false)
          println(" =======  DATAFRAME ANALISE df_prod  =======  ") 
          df_prod.show(false)
          println(" =======  DATAFRAME EXPECTED df_prod  =======  ") 
          dfExpectedProd.show(false)
          println(" =======  DATAFRAME  DIFF df_prod   =======  ") 
          df_prod.except(dfExpectedProd).show(false)
          dfExpectedProd.except(df_prod).show(false)
        }
        
    // Assert ....
        print(" TEST df_bac  ...  ")


        assertEquals(0, dfExpectedBac.except(df_bac).count())
        assertEquals(0, df_bac.except(dfExpectedBac).count())
        println("OK")
        print(" TEST df_prod  ...  ")
        assertEquals(0, dfExpectedProd.except(df_prod).count())
        assertEquals(0, df_prod.except(dfExpectedProd).count())
        println("OK")
      }
      case Left(df) => {
        println("TEST left...")
      }
    }

  }


  @Test
  def ExecuteDechetRefAnalysisTestMultiple(): Unit = {
    println("\n#\n#\n ############################### ExecuteDechetRefAnalysisTest ############################### \n#\n#")
    var directory = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_prod/orc/");
    FileUtils.cleanDirectory(directory);
    directory = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_bac/orc/");
    FileUtils.cleanDirectory(directory);
    val schemaOutputProd = StructType(
        List(
          StructField("id_producteur", LongType, false),
          StructField("code_producteur", StringType, true),
          StructField("id_rva", StringType, true),
          StructField("commune", StringType, true),
          StructField("code_insee", StringType, true),
          StructField("type_producteur", StringType, true),
          StructField("activite", StringType, true),
          StructField("latitude", StringType, true),
          StructField("longitude", StringType, true),
          StructField("date_debut", TimestampType, true),
          StructField("date_fin", TimestampType, true),
          StructField("date_crea", TimestampType, true),
          StructField("date_modif", TimestampType, true),
          StructField("year", StringType, true),
          StructField("month", StringType, true),
          StructField("day", StringType, true)
        )
      )
    val schemaOutputBac = StructType(
        List(
          StructField("id_bac", LongType, false),
          StructField("code_puce", StringType, true),
          StructField("code_producteur", StringType, true),
          StructField("categorie_recipient", StringType, true),
          StructField("type_recipient", StringType, true),
          StructField("litrage_recipient", StringType, true),
          StructField("type_puce", StringType, true),
          StructField("nb_collecte", DoubleType, true),
          StructField("date_debut", TimestampType, true),
          StructField("date_fin", TimestampType, true),
          StructField("date_crea", TimestampType, true),
          StructField("date_modif", TimestampType, true),
          StructField("year", StringType, true),
          StructField("month", StringType, true),
          StructField("day", StringType, true),
          StructField("id_producteur", LongType, false)
        )
      )
    println("LECTURE EXPECTED")
    var dfExpectedBac = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputBac) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Output/bac/year=2022/month=02/day=27/ref_bac_output.csv")
    println("EXPECTED BAC")
    dfExpectedBac.show()
    var dfExpectedProd = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputProd) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Output/prod/year=2022/month=02/day=27/ref_producteur_output.csv")

    println("EXPECTED PROD")
    dfExpectedBac.show()
    var args = Array("2022-02-25","2022-02-25T12:00:00.00Z")
    println("call1 ")
    ExecuteDechetRefAnalysis.main(args)
    var args2 = Array("2022-02-26","2022-02-26T12:00:00.00Z")
    println("call2 ")
    ExecuteDechetRefAnalysis.main(args2)
    var args3 = Array("2022-02-27","2022-02-27T12:00:00.00Z")
    println("call3 ")
    ExecuteDechetRefAnalysis.main(args3) match {
      case Right(df) =>{
        var dfAnalysedProd = spark
          .read
          .option("header", "True")
          .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
          .schema(schemaOutputProd) // mandatory
          .option("delimiter", ";")
          .load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_prod/csv/ref_prod.csv")
        var dfAnalysedBac = spark
          .read
          .option("header", "True")
          .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
          .schema(schemaOutputBac) // mandatory
          .option("delimiter", ";")
          .load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_bac/csv/ref_bac.csv")
        var df_bac = dfAnalysedBac
        var df_prod = dfAnalysedProd

        if(DEBUG == "true"){

          println(" =======  SCHEMA df_bac  ======= ")
          df_bac.printSchema()
          dfExpectedBac.printSchema()
          println(" =======  SCHEMA df_prod  =======  ") 
          df_prod.printSchema()
          dfExpectedProd.printSchema()
          println(" =======  DATAFRAME ANALISE df_bac  =======  ") 
          df_bac.show(false)
          println(" =======  DATAFRAME EXPECTED df_bac  =======  ") 
          dfExpectedBac.show(false)
          println(" =======  DATAFRAME DIFF df_bac   =======  ")
          //dfAnalizedBac.show(false)
          df_bac.except(dfExpectedBac).show(false)
          dfExpectedBac.except(df_bac).show(false)
          println(" =======  DATAFRAME ANALISE df_prod  =======  ") 
          df_prod.show(false)
          println(" =======  DATAFRAME EXPECTED df_prod  =======  ")
          dfExpectedProd.show(false)
          println(" =======  DATAFRAME  DIFF df_prod   =======  ")
          df_prod.except(dfExpectedProd).show(false)
          dfExpectedProd.except(df_prod).show(false)
        }
        
    // Assert ....
        print(" TEST df_bac  ...  ")


        assertEquals(0, dfExpectedBac.except(df_bac).count())
        assertEquals(0, df_bac.except(dfExpectedBac).count())
        println("OK")
        print(" TEST df_prod  ...  ")
        assertEquals(0, dfExpectedProd.except(df_prod).count())
        assertEquals(0, df_prod.except(dfExpectedProd).count())
        println("OK")
      }
      case Left(df) => {
        println("TEST left...")
      }
    }

  }



  @Test
  def ExecuteDechetRefAnalysisTest(): Unit = {
    var directory = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_prod/orc/");
    FileUtils.cleanDirectory(directory);
    directory = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_bac/orc/");
    FileUtils.cleanDirectory(directory);
    println("\n#\n#\n ############################### ExecuteDechetRefAnalysisTest ############################### \n#\n#")
    val schemaOutputProd = StructType(
      List(
        StructField("id_producteur", LongType, false),
        StructField("code_producteur", StringType, true),
        StructField("id_rva", StringType, true),
        StructField("commune", StringType, true),
        StructField("code_insee", StringType, true),
        StructField("type_producteur", StringType, true),
        StructField("activite", StringType, true),
        StructField("latitude", StringType, true),
        StructField("longitude", StringType, true),
        StructField("date_debut", TimestampType, true),
        StructField("date_fin", TimestampType, true),
        StructField("date_crea", TimestampType, true),
        StructField("date_modif", TimestampType, true),
        StructField("year", StringType, true),
        StructField("month", StringType, true),
        StructField("day", StringType, true)
      )
    )
    val schemaOutputBac = StructType(
      List(
        StructField("id_bac", LongType, false),
        StructField("code_puce", StringType, true),
        StructField("code_producteur", StringType, true),
        StructField("categorie_recipient", StringType, true),
        StructField("type_recipient", StringType, true),
        StructField("litrage_recipient", StringType, true),
        StructField("type_puce", StringType, true),
        StructField("nb_collecte", DoubleType, true),
        StructField("date_debut", TimestampType, true),
        StructField("date_fin", TimestampType, true),
        StructField("date_crea", TimestampType, true),
        StructField("date_modif", TimestampType, true),
        StructField("year", StringType, true),
        StructField("month", StringType, true),
        StructField("day", StringType, true),
        StructField("id_producteur", LongType, false)
      )
    )
    val dfExpectedBac = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputBac) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Output/bac/year=2022/month=03/day=01/ExecuteDechetRefAnalysisTestOutputBacWithHeader.csv")

    val dfExpectedProd = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputProd) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Output/prod/year=2022/month=03/day=01/ExecuteDechetRefAnalysisTestOutputProdWithHeader.csv")

    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-03-01","2022-03-01T12:00:00.00Z")
    ExecuteDechetRefAnalysis.main(args) match {
      case Right(df) =>{
        val df_bac = df._1
        val df_prod = df._2
        //LOG DEBUG
        if(DEBUG == "true"){
          println(" =======  SCHEMA df_bac  =======  ") 
          df_bac.printSchema()
          println(" =======  SCHEMA df_prod  =======  ") 
          df_prod.printSchema()
          println(" =======  DATAFRAME ANALYSE df_bac  =======  ") 
          df_bac.show(false)
          println(" =======  DATAFRAME EXPECTED df_bac  =======  ") 
          dfExpectedBac.show(false)
          println(" =======  DATAFRAME DIFF df_bac   =======  ")
          println("ligne que je ne devrais pas avoir mais que j'ai  :")
          df_bac.except(dfExpectedBac).show(false)
          println("ligne que je devrais avoir mais que je n'ai pas :")
          dfExpectedBac.except(df_bac).show(false)
          println(" =======  DATAFRAME ANALYSE df_prod  =======  ") 
          df_prod.show(false)
          println(" =======  DATAFRAME EXPECTED df_prod  =======  ") 
          dfExpectedProd.show(false)
          println(" =======  DATAFRAME  DIFF df_prod   =======  ")
          println("ligne que je ne devrais pas avoir mais que j'ai  :")
          df_prod.except(dfExpectedProd).show(false)
          println("ligne que je devrais avoir mais que je n'ai pas :")
          dfExpectedProd.except(df_prod).show(false)
        }
        
    // Assert ....
        print(" TEST df_bac  ...  ")
        assertEquals(0, dfExpectedBac.except(df_bac).count())
        assertEquals(0, df_bac.except(dfExpectedBac).count())
        println("OK")
        print(" TEST df_prod  ...  ")
        assertEquals(0, dfExpectedProd.except(df_prod).count())
        assertEquals(0, df_prod.except(dfExpectedProd).count())
        println("OK")
      }
      case Left(df) => {
        println("TEST left...")
      }
    }
  }

  @Test
  def ExecuteDechetRefAnalysisTest_EmptyFile(): Unit = {
    println("\n#\n#\n ############################### ExecuteDechetRefAnalysisTest_EmptyFile ############################### \n#\n#")
    var directory = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_prod/orc/");
    FileUtils.cleanDirectory(directory);
    directory = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_bac/orc/");
    FileUtils.cleanDirectory(directory);

    val schemaOutputProd = StructType(
    List(
      StructField("id_producteur", LongType, false),
      StructField("code_producteur", StringType, true),
      StructField("id_rva", StringType, true),
      StructField("commune", StringType, true),
      StructField("code_insee", StringType, true),
      StructField("type_producteur", StringType, true),
      StructField("activite", StringType, true),
      StructField("latitude", StringType, true),
      StructField("longitude", StringType, true),
      StructField("date_debut", TimestampType, true),
      StructField("date_fin", TimestampType, true),
      StructField("date_crea", TimestampType, true),
      StructField("date_modif", TimestampType, true),
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true)
    )
  )
  val schemaOutputBac = StructType(
    List(
      StructField("id_bac", LongType, false),
      StructField("code_puce", StringType, true),
      StructField("code_producteur", StringType, true),
      StructField("categorie_recipient", StringType, true),
      StructField("type_recipient", StringType, true),
      StructField("litrage_recipient", StringType, true),
      StructField("type_puce", StringType, true),
      StructField("nb_collecte", DoubleType, true),
      StructField("date_debut", TimestampType, true),
      StructField("date_fin", TimestampType, true),
      StructField("date_crea", TimestampType, true),
      StructField("date_modif", TimestampType, true),
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true),
      StructField("id_producteur", LongType, false)

    )
  )

    val dfExpectedBac = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputBac) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Output/bac/year=2022/month=03/day=02/ExecuteDechetRefAnalysisTestOutputBacWithHeaderEmpty.csv")

    val dfExpectedProd = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputProd) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Output/prod/year=2022/month=03/day=02/ExecuteDechetRefAnalysisTestOutputProdWithHeaderEmpty.csv")

        
    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-03-02")
    ExecuteDechetRefAnalysis.main(args) match {
      case Right(df) =>{
        val df_bac = df._1
        val df_prod = df._2
        //LOG DEBUG
        if(DEBUG == "true"){
          println(" =======  SCHEMA df_bac  =======  ") 
          df_bac.printSchema()
          println(" =======  SCHEMA df_prod  =======  ") 
          df_prod.printSchema()
          println(" =======  DATAFRAME ANALYSE df_bac  =======  ") 
          df_bac.show(false)
          println(" =======  DATAFRAME EXPECTED df_bac  =======  ") 
          dfExpectedBac.show(false)
          println(" =======  DATAFRAME DIFF df_bac   =======  ") 
          df_bac.except(dfExpectedBac).show(false)
          println(" =======  DATAFRAME ANALYSE df_prod  =======  ") 
          df_prod.show(false)
          println(" =======  DATAFRAME EXPECTED df_prod  =======  ") 
          dfExpectedProd.show(false)
          println(" =======  DATAFRAME  DIFF df_prod   =======  ") 
          df_prod.except(dfExpectedProd).show(false)
        }
        
        // Assert ....
        print(" TEST df_bac  ...  ")
        assertEquals(0, dfExpectedBac.except(df_bac).count())
        assertEquals(0, df_bac.except(dfExpectedBac).count())
        println("OK")
        print(" TEST df_prod  ...  ")
        assertEquals(0, dfExpectedProd.except(df_prod).count())
        assertEquals(0, df_prod.except(dfExpectedProd).count())
        println("OK")
      }
      case Left(df) => {
        println("TEST left...")
      }
    }
  }

  @Test
  def ExecuteDechetRefAnalysisTest_CodeDoublons(): Unit = {
    println("\n#\n#\n ############################### ExecuteDechetRefAnalysisTest_EmptyFile ############################### \n#\n#")
    var directory = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_prod/orc/");
    FileUtils.cleanDirectory(directory);
    directory = new File("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_bac/orc/");
    FileUtils.cleanDirectory(directory);

    val schemaOutputProd = StructType(
      List(
        StructField("id_producteur", LongType, false),
        StructField("code_producteur", StringType, true),
        StructField("id_rva", StringType, true),
        StructField("commune", StringType, true),
        StructField("code_insee", StringType, true),
        StructField("type_producteur", StringType, true),
        StructField("activite", StringType, true),
        StructField("latitude", StringType, true),
        StructField("longitude", StringType, true),
        StructField("date_debut", TimestampType, true),
        StructField("date_fin", TimestampType, true),
        StructField("date_crea", TimestampType, true),
        StructField("date_modif", TimestampType, true),
        StructField("year", StringType, true),
        StructField("month", StringType, true),
        StructField("day", StringType, true)
      )
    )
    val schemaOutputBac = StructType(
      List(
        StructField("id_bac", LongType, false),
        StructField("code_puce", StringType, true),
        StructField("code_producteur", StringType, true),
        StructField("categorie_recipient", StringType, true),
        StructField("type_recipient", StringType, true),
        StructField("litrage_recipient", StringType, true),
        StructField("type_puce", StringType, true),
        StructField("nb_collecte", DoubleType, true),
        StructField("date_debut", TimestampType, true),
        StructField("date_fin", TimestampType, true),
        StructField("date_crea", TimestampType, true),
        StructField("date_modif", TimestampType, true),
        StructField("year", StringType, true),
        StructField("month", StringType, true),
        StructField("day", StringType, true),
        StructField("id_producteur", LongType, false)

      )
    )

    val dfExpectedBac = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputBac) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Output/bac/year=2022/month=03/day=07/ExecuteDechetRefAnalysisTestOutputBacDoublonCode.csv")

    val dfExpectedProd = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputProd) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Output/prod/year=2022/month=03/day=07/ExecuteDechetRefAnalysisTestOutputProdDoublonCode.csv")


    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-03-07","2022-03-07T12:00:00.00Z")
    ExecuteDechetRefAnalysis.main(args) match {
      case Right(df) =>{
        val df_bac = df._1
        val df_prod = df._2
        //LOG DEBUG
        if(DEBUG == "true"){
          println(" =======  SCHEMA df_bac  =======  ")
          df_bac.printSchema()
          println(" =======  SCHEMA df_prod  =======  ")
          df_prod.printSchema()
          println(" =======  DATAFRAME ANALYSE df_bac  =======  ")
          df_bac.show(false)
          println(" =======  DATAFRAME EXPECTED df_bac  =======  ")
          dfExpectedBac.show(false)
          println(" =======  DATAFRAME DIFF df_bac   =======  ")
          df_bac.except(dfExpectedBac).show(false)
          println(" =======  DATAFRAME ANALYSE df_prod  =======  ")
          df_prod.show(false)
          println(" =======  DATAFRAME EXPECTED df_prod  =======  ")
          dfExpectedProd.show(false)
          println(" =======  DATAFRAME  DIFF df_prod   =======  ")
          df_prod.except(dfExpectedProd).show(false)
        }

        // Assert ....
        print(" TEST df_bac  ...  ")
        assertEquals(0, dfExpectedBac.except(df_bac).count())
        assertEquals(0, df_bac.except(dfExpectedBac).count())
        println("OK")
        print(" TEST df_prod  ...  ")
        assertEquals(0, dfExpectedProd.except(df_prod).count())
        assertEquals(0, df_prod.except(dfExpectedProd).count())
        println("OK")
      }
      case Left(df) => {
        println("TEST left...")
      }
    }
  }
}