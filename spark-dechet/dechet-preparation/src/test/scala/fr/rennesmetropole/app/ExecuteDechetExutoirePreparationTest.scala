 package fr.rennesmetropole.app

 import com.typesafe.config.ConfigFactory
 import fr.rennesmetropole.tools.Utils
 import org.apache.spark.sql._
 import org.apache.spark.sql.functions._
 import org.apache.spark.sql.types._
 import org.junit.Assert._
 import org.junit.Test
 import org.scalatest.Assertions._

 class ExecuteDechetExutoirePreparationTest {
   val config = ConfigFactory.load()
   val DEBUG = Utils.envVar("TEST_DEBUG")
   @Test
   def ExecuteDechetExutoirePreparationTest(): Unit = {
     val spark: SparkSession = SparkSession.builder()
       .master("local[1]")
       .appName("SparkTests")
       .getOrCreate()

     val schemaOutput = StructType(
       List(
         StructField("immat", StringType, false), //Immat.
         StructField("date_service_vehic", StringType, true), //Date service véhic.
         StructField("description_tournee", StringType, true), //Description tournée
         StructField("km_realise", IntegerType, false), //Km réalisé
         StructField("no_bon", StringType, true), //noBon filtre pour doublon
         StructField("lot", StringType, true), //LOT
         StructField("service", IntegerType, false), //Service
         StructField("nom_rech_lieu_de_vidage", StringType, true), //Nom rech. lieu de vidage
         StructField("multiples_lignes", DoubleType, true), //Nom multiples ligne
         StructField("cle_unique_ligne_ticket", StringType, true) //, //Clé unique Ligne ticket
         )
     )
     val dfExpected = spark
       .read
       .option("header", "true")
       .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
       .schema(schemaOutput) // mandatory
       .option("delimiter", ";")
       .load("src/test/resources/Local/app/ExecuteDechetExutoirePreparation/Output/year=2022/month=01/day=01/ExecuteDechetExutoirePreparationTestOutput.csv")
     println("df_expected")
     dfExpected.show()
     spark.sparkContext.setLogLevel("WARN")
     val args = Array("2022-01-01")
     ExecuteDechetExutoirePreparation.main(args) match {
     case Right(dfPrepared) => {
       if(DEBUG == "true"){
         println(" =======  DATAFRAME PREPARED df_bac  =======  ")
         dfPrepared.show(false)
         println(" =======  DATAFRAME EXPECTED df_bac  =======  ")
         dfExpected.show(false)
         println(" =======  DATAFRAME DIFF df_exutoire  =======  ")
         println("ligne que je ne devrais pas avoir mais que j'ai  :")
         dfPrepared.except(dfExpected).show(false)
         println("ligne que je devrais avoir mais que je n'ai pas :")
         dfExpected.except(dfPrepared).show(false)
       }
       print(" TEST df_exutoire  ...  ")
       assertEquals(0, dfPrepared.except(dfExpected).count())
       assertEquals(0, dfExpected.except(dfPrepared).count())
       println("OK")
     }
     }
   }

   @Test
    def ExecuteDechetExutoirePreparationTestExcelSimple(): Unit = {
     val spark: SparkSession = SparkSession.builder()
       .master("local[1]")
       .appName("SparkTests")
       .getOrCreate()

     val schemaOutput = StructType(
       List(
         StructField("immat", StringType, false), //Immat.
         StructField("date_service_vehic", StringType, true), //Date service véhic.
         StructField("description_tournee", StringType, true), //Description tournée
         StructField("km_realise", IntegerType, false), //Km réalisé
         StructField("no_bon", StringType, true), //noBon filtre pour doublon
         StructField("lot", StringType, true), //LOT
         StructField("service", IntegerType, false), //Service
         StructField("nom_rech_lieu_de_vidage", StringType, true), //Nom rech. lieu de vidage
         StructField("multiples_lignes", DoubleType, true), //Nom multiples ligne
         StructField("cle_unique_ligne_ticket", StringType, true) //, //Clé unique Ligne ticket
       )
     )
     val dfExpected = spark
       .read
       .option("header", "true")
       .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
       .schema(schemaOutput) // mandatory
       .option("delimiter", ";")
       .load("src/test/resources/Local/app/ExecuteDechetExutoirePreparation/Output/year=2022/month=01/day=02/ExecuteDechetExutoirePreparationExcelSimpleTestOutput.csv")

     println("df_expected")
     dfExpected.show()
     spark.sparkContext.setLogLevel("WARN")
     val args = Array("2022-01-02")
     ExecuteDechetExutoirePreparation.main(args) match {
       case Right(dfPrepared) => {
         if (DEBUG == "true") {
           println(" =======  DATAFRAME PREPARED df_bac  =======  ")
           dfPrepared.show(false)
           println(" =======  DATAFRAME EXPECTED df_bac  =======  ")
           dfExpected.show(false)
           println(" =======  DATAFRAME DIFF df_exutoire  =======  ")
           println("ligne que je ne devrais pas avoir mais que j'ai  :")
           dfPrepared.except(dfExpected).show(false)
           println("ligne que je devrais avoir mais que je n'ai pas :")
           dfExpected.except(dfPrepared).show(false)
         }
         print(" TEST df_exutoire  ...  ")
         assertEquals(0, dfPrepared.except(dfExpected).count())
         assertEquals(0, dfExpected.except(dfPrepared).count())
         println("OK")
       }
     }
   }

   @Test
   def ExecuteDechetExutoirePreparationTestExcelComplexe(): Unit = {
     val spark: SparkSession = SparkSession.builder()
       .master("local[1]")
       .appName("SparkTests")
       .getOrCreate()

     val schemaOutput = StructType(
       List(
         StructField("immat", StringType, false), //Immat.
         StructField("date_service_vehic", StringType, true), //Date service véhic.
         StructField("description_tournee", StringType, true), //Description tournée
         StructField("km_realise", IntegerType, false), //Km réalisé
         StructField("no_bon", StringType, true), //noBon filtre pour doublon
         StructField("lot", StringType, true), //LOT
         StructField("service", IntegerType, false), //Service
         StructField("nom_rech_lieu_de_vidage", StringType, true), //Nom rech. lieu de vidage
         StructField("multiples_lignes", DoubleType, true), //Nom multiples ligne
         StructField("cle_unique_ligne_ticket", StringType, true) //, //Clé unique Ligne ticket
       )
     )
     /*var dfExpected = spark
       .read
       .format("com.crealytics.spark.excel")
       .option("header", "true")
       .option("inferSchema", "false")
       .option("dataAddress", "'Données brutes Clear'!")
       .load("src/test/resources/Local/app/ExecuteDechetExutoirePreparation/Output/year=2022/month=01/day=03/ExecuteDechetExutoirePreparationTestExcelComplexeOutput.xlsx")
     dfExpected = Utils.regexCharSpe(spark, dfExpected)
       .select("immat","date_service_vehic","description_tournee","km_realise","no_bon","lot","service","nom_rech_lieu_de_vidage","multiples_lignes","cle_unique_ligne_ticket")
     println("DF EXPECTED")
     dfExpected.show()*/
     val dfExpected = spark
       .read
       .option("header", "true")
       .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
       .schema(schemaOutput) // mandatory
       .option("delimiter", ";")
       .load("src/test/resources/Local/app/ExecuteDechetExutoirePreparation/Output/year=2022/month=01/day=03/ExecuteDechetExutoirePreparationTestExcelComplexeOutput.csv")

     spark.sparkContext.setLogLevel("WARN")
     val args = Array("2022-01-03")
     ExecuteDechetExutoirePreparation.main(args) match {
       case Right(dfPrepared) => {
         if(DEBUG == "true"){
           dfPrepared.printSchema()
           dfExpected.printSchema()
           println(" =======  DATAFRAME PREPARED df_bac  =======  ")
           dfPrepared.show(false)
           println(" =======  DATAFRAME EXPECTED df_bac  =======  ")
           dfExpected.show(false)
           println(" =======  DATAFRAME DIFF df_exutoire  =======  ")
           println("ligne que je ne devrais pas avoir mais que j'ai  :")
           dfPrepared.except(dfExpected).show(false)
           println("ligne que je devrais avoir mais que je n'ai pas :")
           dfExpected.except(dfPrepared).show(false)
         }
        /* dfPrepared   // nécessaire pour écrire le DF dans un seul csv, mais pPeut poser problème si le DF est trop gros
           .write.options(Map("header"->"true", "delimiter"->";"))
           .mode(SaveMode.Append)
           .csv("src/test/resources/Local/app/ExecuteDechetExutoirePreparation/Output/year=2022/month=01/day=03/csv")
         */print(" TEST df_exutoire  ...  ")
         assertEquals(0, dfPrepared.except(dfExpected).count())
         assertEquals(0, dfExpected.except(dfPrepared).count())
         println("OK")
       }
     }
   }

 }