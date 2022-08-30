package fr.rennesmetropole.app

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession,Row}
import java.util.Arrays.equals
import fr.rennesmetropole.tools.Utils

class ExecuteDechetRefBacPreparationTest {
  val spark: SparkSession = SparkSession.builder()
  .master("local[1]")
  .appName("SparkTests")
  .getOrCreate()
  val config = ConfigFactory.load()
  val DEBUG = Utils.envVar("TEST_DEBUG")

  @Test
  def ExecuteDechetRefPreparationTest(): Unit = {

    println("\n#\n#\n ############################### ExecuteDechetRefPreparationTest ############################### \n#\n#")
    val schemaOutputBac = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("categorie_recipient", StringType, false),
          StructField("type_recipient", StringType, false),
          StructField("litrage_recipient", IntegerType, false),
          StructField("code_puce", StringType, false),
          StructField("frequence_om", IntegerType, false),
          StructField("frequence_cs", DoubleType, false),  //FloatType
          StructField("date_photo", StringType, false)
        )
      )
    val schemaOutputProd = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("rva_adr", IntegerType, true),
          StructField("code_insee", IntegerType, true),
          StructField("nom_commune", StringType, false),
          StructField("type_producteur", StringType, true),
          StructField("activite", StringType, false), 
          StructField("longitude", DoubleType, true),
          StructField("latitude", DoubleType, true),
          StructField("date_photo", StringType, false)
        )
      )

    val dfExpectedBac = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputBac) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefPreparation/Output/bac/year=2022/month=01/day=01/ExecuteDechetRefPreparationTestOutputBacWithHeader.csv")

    val dfExpectedProd = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputProd) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefPreparation/Output/prod/year=2022/month=01/day=01/ExecuteDechetRefPreparationTestOutputProdWithHeader.csv")

    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-01-01")
    ExecuteDechetRefPreparation.main(args) match {
      case Right(df) =>{
        val df_bac = df._1
        val df_prod = df._2
        //LOG DEBUG
        if(DEBUG == "true"){
          println(" =======  SCHEMA df_bac  =======  ") 
          df_bac.printSchema()
          println(" =======  SCHEMA df_prod  =======  ") 
          df_prod.printSchema()
          println(" =======  DATAFRAME PREPARE df_bac  =======  ") 
          df_bac.show(false)
          println(" =======  DATAFRAME EXPECTED df_bac  =======  ") 
          dfExpectedBac.show(false)
          println(" =======  DATAFRAME DIFF df_bac   =======  ") 
          df_bac.except(dfExpectedBac).show(false)
          println(" =======  DATAFRAME PREPARE df_prod  =======  ") 
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
    }
  }

@Test
  def ExecuteDechetRefPreparationTest_EmptyFile(): Unit = {
    println("\n#\n#\n ############################### ExecuteDechetRefPreparationTest_EmptyFile ############################### \n#\n#")
    val schemaOutputBac = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("categorie_recipient", StringType, false),
          StructField("type_recipient", StringType, false),
          StructField("litrage_recipient", IntegerType, false),
          StructField("code_puce", StringType, false),
          StructField("frequence_om", IntegerType, false),
          StructField("frequence_cs", DoubleType, false),  //FloatType
          StructField("date_photo", StringType, false)
        )
      )
    val schemaOutputProd = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("rva_adr", IntegerType, true),
          StructField("code_insee", IntegerType, true),
          StructField("nom_commune", StringType, false),
          StructField("type_producteur", StringType, true),
          StructField("activite", StringType, false), 
          StructField("longitude", DoubleType, true),
          StructField("latitude", DoubleType, true),
          StructField("date_photo", StringType, false)
        )
      )

    val dfExpectedBac = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputBac) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefPreparation/Output/bac/year=2022/month=01/day=02/ExecuteDechetRefPreparationTestOutputBacWithHeaderEmpty.csv")

    val dfExpectedProd = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputProd) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefPreparation/Output/prod/year=2022/month=01/day=02/ExecuteDechetRefPreparationTestOutputProdWithHeaderEmpty.csv")

        
    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-01-02")
    ExecuteDechetRefPreparation.main(args) match {
      case Right(df) =>{
        val df_bac = df._1
        val df_prod = df._2
        //LOG DEBUG
        if(DEBUG == "true"){
          println(" =======  SCHEMA df_bac  =======  ") 
          df_bac.printSchema()
          println(" =======  SCHEMA df_prod  =======  ") 
          df_prod.printSchema()
          println(" =======  DATAFRAME PREPARE df_bac  =======  ") 
          df_bac.show(false)
          println(" =======  DATAFRAME EXPECTED df_bac  =======  ") 
          dfExpectedBac.show(false)
          println(" =======  DATAFRAME DIFF df_bac   =======  ") 
          df_bac.except(dfExpectedBac).show(false)
          println(" =======  DATAFRAME PREPARE df_prod  =======  ") 
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
    }
  }

  @Test
  def ExecuteDechetRefPreparationTest_WeirdColumn(): Unit = {
    println("\n#\n#\n ############################### ExecuteDechetRefPreparationTest_WeirdColumn ############################### \n#\n#")
     val schemaOutputBac = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("categorie_recipient", StringType, false),
          StructField("type_recipient", StringType, false),
          StructField("litrage_recipient", IntegerType, false),
          StructField("code_puce", StringType, false),
          StructField("frequence_om", IntegerType, false),
          StructField("frequence_cs", DoubleType, false),  //FloatType
          StructField("date_photo", StringType, false)
        )
      )
    val schemaOutputProd = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("rva_adr", IntegerType, true),
          StructField("code_insee", IntegerType, true),
          StructField("nom_commune", StringType, false),
          StructField("type_producteur", StringType, true),
          StructField("activite", StringType, false), 
          StructField("longitude", DoubleType, true),
          StructField("latitude", DoubleType, true),
          StructField("date_photo", StringType, false)
        )
      )
    val dfExpectedBac = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputBac) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefPreparation/Output/bac/year=2022/month=01/day=03/ExecuteDechetRefPreparationTestOutputBacWithHeader.csv")

    val dfExpectedProd = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputProd) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefPreparation/Output/prod/year=2022/month=01/day=03/ExecuteDechetRefPreparationTestOutputProdWithHeaderEmpty.csv")


    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-01-03")
    ExecuteDechetRefPreparation.main(args) match {
      case Right(df) =>{
        val df_bac = df._1
        val df_prod = df._2
        //LOG DEBUG
        if(DEBUG == "true"){
          println(" =======  SCHEMA df_bac  =======  ") 
          df_bac.printSchema()
          println(" =======  SCHEMA df_prod  =======  ") 
          df_prod.printSchema()
          println(" =======  DATAFRAME PREPARE df_bac  =======  ") 
          df_bac.show(false)
          println(" =======  DATAFRAME EXPECTED df_bac  =======  ") 
          dfExpectedBac.show(false)
          println(" =======  DATAFRAME DIFF df_bac   =======  ") 
          df_bac.except(dfExpectedBac).show(false)
          println(" =======  DATAFRAME PREPARE df_prod  =======  ") 
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
    }
  }

  @Test
  def ExecuteDechetRefPreparationTest_WeirdValue(): Unit = {
    println("\n#\n#\n ############################### ExecuteDechetRefPreparationTest_WeirdValue ############################### \n#\n#")
     val schemaOutputBac = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("categorie_recipient", StringType, false),
          StructField("type_recipient", StringType, false),
          StructField("litrage_recipient", IntegerType, false),
          StructField("code_puce", StringType, false),
          StructField("frequence_om", IntegerType, false),
          StructField("frequence_cs", DoubleType, false),  //FloatType
          StructField("date_photo", StringType, false)
        )
      )
    val schemaOutputProd = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("rva_adr", IntegerType, true),
          StructField("code_insee", IntegerType, true),
          StructField("nom_commune", StringType, false),
          StructField("type_producteur", StringType, true),
          StructField("activite", StringType, false), 
          StructField("longitude", DoubleType, true),
          StructField("latitude", DoubleType, true),
          StructField("date_photo", StringType, false)
        )
      )

    val dfExpectedBac = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputBac) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefPreparation/Output/bac/year=2022/month=01/day=04/ExecuteDechetRefPreparationTestOutputBacWithHeader.csv")

    val dfExpectedProd = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputProd) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefPreparation/Output/prod/year=2022/month=01/day=04/ExecuteDechetRefPreparationTestOutputProdWithHeaderEmpty.csv")


    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-01-04")
    ExecuteDechetRefPreparation.main(args) match {
      case Right(df) =>{
        val df_bac = df._1
        val df_prod = df._2
        //LOG DEBUG
        if(DEBUG == "true"){
          println(" =======  SCHEMA df_bac  =======  ") 
          df_bac.printSchema()
          println(" =======  SCHEMA df_prod  =======  ") 
          df_prod.printSchema()
          println(" =======  DATAFRAME PREPARE df_bac  =======  ") 
          df_bac.show(false)
          println(" =======  DATAFRAME EXPECTED df_bac  =======  ") 
          dfExpectedBac.show(false)
          println(" =======  DATAFRAME DIFF df_bac   =======  ") 
          df_bac.except(dfExpectedBac).show(false)
          println(" =======  DATAFRAME PREPARE df_prod  =======  ") 
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
    }
  }

  @Test
  def ExecuteDechetRefPreparationTest_WithoutValue(): Unit = {
     println("\n#\n#\n ############################### ExecuteDechetRefPreparationTest_WithoutValue ############################### \n#\n#")
     val schemaOutputBac = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("categorie_recipient", StringType, false),
          StructField("type_recipient", StringType, false),
          StructField("litrage_recipient", IntegerType, false),
          StructField("code_puce", StringType, false),
          StructField("frequence_om", IntegerType, false),
          StructField("frequence_cs", DoubleType, false),  //FloatType
          StructField("date_photo", StringType, false)
        )
      )
    val schemaOutputProd = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("rva_adr", IntegerType, true),
          StructField("code_insee", IntegerType, true),
          StructField("nom_commune", StringType, false),
          StructField("type_producteur", StringType, true),
          StructField("activite", StringType, false), 
          StructField("longitude", DoubleType, true),
          StructField("latitude", DoubleType, true),
          StructField("date_photo", StringType, false)
        )
      )

    val dfExpectedBac = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputBac) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefPreparation/Output/bac/year=2022/month=01/day=05/ExecuteDechetRefPreparationTestOutputBacWithHeaderEmpty.csv")

    val dfExpectedProd = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputProd) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefPreparation/Output/prod/year=2022/month=01/day=05/ExecuteDechetRefPreparationTestOutputProdWithHeaderEmpty.csv")


    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-01-05")
    ExecuteDechetRefPreparation.main(args) match {
      case Right(df) =>{
        val df_bac = df._1
        val df_prod = df._2
        //LOG DEBUG
        if(DEBUG == "true"){
          println(" =======  SCHEMA df_bac  =======  ") 
          df_bac.printSchema()
          println(" =======  SCHEMA df_prod  =======  ") 
          df_prod.printSchema()
          println(" =======  DATAFRAME PREPARE df_bac  =======  ") 
          df_bac.show(false)
          println(" =======  DATAFRAME EXPECTED df_bac  =======  ") 
          dfExpectedBac.show(false)
          println(" =======  DATAFRAME DIFF df_bac   =======  ") 
          df_bac.except(dfExpectedBac).show(false)
          println(" =======  DATAFRAME PREPARE df_prod  =======  ") 
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
    }
  }

  @Test
  def ExecuteDechetRefPreparationTest_Wrongtype(): Unit = {
     println("\n#\n#\n ############################### ExecuteDechetRefPreparationTest_Wrongtype ############################### \n#\n#")
     val schemaOutputBac = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("categorie_recipient", StringType, false),
          StructField("type_recipient", StringType, false),
          StructField("litrage_recipient", IntegerType, false),
          StructField("code_puce", StringType, false),
          StructField("frequence_om", IntegerType, false),
          StructField("frequence_cs", DoubleType, false),  //FloatType
          StructField("date_photo", StringType, false)
        )
      )
    val schemaOutputProd = StructType(
        List(
          StructField("code_producteur", IntegerType, false),
          StructField("rva_adr", IntegerType, true),
          StructField("code_insee", IntegerType, true),
          StructField("nom_commune", StringType, false),
          StructField("type_producteur", StringType, true),
          StructField("activite", StringType, false), 
          StructField("longitude", DoubleType, true),
          StructField("latitude", DoubleType, true),
          StructField("date_photo", StringType, false)
        )
      )

    val dfExpectedBac = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputBac) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefPreparation/Output/bac/year=2022/month=01/day=06/ExecuteDechetRefPreparationTestOutputBacWithHeaderEmpty.csv")

    val dfExpectedProd = spark
      .read
      .option("header", "True")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutputProd) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/app/ExecuteDechetRefPreparation/Output/prod/year=2022/month=01/day=06/ExecuteDechetRefPreparationTestOutputProdWithHeaderEmpty.csv")


    spark.sparkContext.setLogLevel("WARN")
    val args = Array("2022-01-06")
    ExecuteDechetRefPreparation.main(args) match {
      case Right(df) =>{
        val df_bac = df._1
        val df_prod = df._2
        //LOG DEBUG
        if(DEBUG == "true"){
          println(" =======  SCHEMA df_bac  =======  ") 
          df_bac.printSchema()
          println(" =======  SCHEMA df_prod  =======  ") 
          df_prod.printSchema()
          println(" =======  DATAFRAME PREPARE df_bac  =======  ") 
          df_bac.show(false)
          println(" =======  DATAFRAME EXPECTED df_bac  =======  ") 
          dfExpectedBac.show(false)
          println(" =======  DATAFRAME DIFF df_bac   =======  ") 
          df_bac.except(dfExpectedBac).show(false)
          println(" =======  DATAFRAME PREPARE df_prod  =======  ") 
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
    }
  }

}