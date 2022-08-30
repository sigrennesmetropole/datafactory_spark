package fr.rennesmetropole.services

import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Assert._
import org.junit.Test

class DechetAnalysisTest {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkTests")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")


  @Test
  def ExecuteDechetAnalysisTest(): Unit = {
    val schemaInput = StructType(
      List(
        StructField("Date_de_la_levee", StringType, false),
        StructField("Heure_de_la_levee", StringType, false),
        StructField("Code_puce", StringType, true),
        StructField("Pesee_net", DoubleType, true),
        StructField("Statut_de_la_levee", IntegerType, false),
        StructField("Latitude", DoubleType, true),
        StructField("Longitude", DoubleType, true),
        StructField("Bouton_poussoir_1", DoubleType, true),
        StructField("Bouton_poussoir_2", DoubleType, true),
        StructField("Bouton_poussoir_3", DoubleType, true),
        StructField("Bouton_poussoir_4", DoubleType, true),
        StructField("Bouton_poussoir_5", DoubleType, true),
        StructField("Bouton_poussoir_6", DoubleType, true),
        StructField("Statut_du_bac", StringType, true),
        StructField("Immatriculation ", StringType, true),
        StructField("Tournee", StringType, true)
      )
    )
    var dfInput =  spark
        .read
        .option("header", "true")
        .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
        .schema(schemaInput) // mandatory
        .option("delimiter", ";")
        .load("src/test/resources/Local/services/Analysis/DechetAnalysisInput.csv")

    val schemaOutput = StructType(
      List(
        StructField("Date_de_la_levee", StringType, false),
        StructField("Heure_de_la_levee", StringType, false),
        StructField("Code_puce", StringType, true),
        StructField("Pesee_net", DoubleType, true),
        StructField("Statut_de_la_levee", IntegerType, false),
        StructField("Latitude", DoubleType, true),
        StructField("Longitude", DoubleType, true),
        StructField("date_crea", StringType, false),
        StructField("date_modif", StringType, false),
        StructField("year", StringType, false),
        StructField("month", StringType, false),
        StructField("day", StringType, false)
      )
    )

    var dfBac =  spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/latestBac.csv")

    var dfExpected =  spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .schema(schemaOutput) // mandatory
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/DechetAnalysisOutput.csv")
    val dfOutput = DechetAnalysis.ExecuteDechetAnalysis_Collecte(spark,dfInput,"2021-01-01", dfBac)
    assertEquals(0, dfOutput.except(dfExpected).count())
  }

 @Test
  def selectEntryToUpdateDeleteTest(): Unit = {
   var dfInputLastest =  spark
     .read
     .option("header", "true")
     .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
     .option("delimiter", ";")
     .load("src/test/resources/Local/services/Analysis/selectEntryToUpdateDeleteTest/dfInputLastest.csv")

   var dfInputNew =  spark
     .read
     .option("header", "true")
     .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
     .option("delimiter", ";")
     .load("src/test/resources/Local/services/Analysis/selectEntryToUpdateDeleteTest/dfInputNew.csv")

   var dfExpected = spark
     .read
     .option("header", "true")
     .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
     .option("delimiter", ";")
     .load("src/test/resources/Local/services/Analysis/selectEntryToUpdateDeleteTest/dfExpected.csv")

   val result = DechetAnalysis.selectEntryToUpdateDeleted(dfInputNew, dfInputLastest, "Code_puce")
   assertEquals(0, result.except(dfExpected).count())
  }

  @Test
  def selectEntryToAddToReferentialTest(): Unit = {
    var dfInputLastest = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/selectEntryToAddToReferentialTest/dfInputLastest.csv")

    var dfInputNew = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/selectEntryToAddToReferentialTest/dfInputNew.csv")

    var dfExpected = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/selectEntryToAddToReferentialTest/dfExpected.csv")

    val result = DechetAnalysis.selectEntryToAddToReferential(dfInputNew, dfInputLastest, "Code_puce")
    assertEquals(0, result.except(dfExpected).count())
  }

  @Test
  def createEmptyBacDfTest(): Unit = {
    val result = DechetAnalysis.createEmptyBacDf(spark)
    val schema = StructType(
      List(
        StructField("id_bac", IntegerType, false),
        StructField("code_puce", StringType, false),
        StructField("code_producteur", IntegerType, false),
        StructField("categorie_recipient", StringType, false),
        StructField("type_recipient", StringType, false),
        StructField("litrage_recipient", IntegerType, false),
        StructField("type_puce", StringType, true),
        StructField("nb_collecte", FloatType, false),
        StructField("id_producteur", IntegerType, false),
        StructField("date_debut", TimestampType, false),
        StructField("date_fin", TimestampType, true),
        StructField("date_crea", TimestampType, false),
        StructField("date_modif", TimestampType, true)
      )
    )
    assertEquals(schema, result.schema)
    assertTrue(result.isEmpty)
  }

  @Test
  def createEmptyProducteurDfTest(): Unit = {
    val result = DechetAnalysis.createEmptyProducteurDf(spark)
    val schema = StructType(
      List(
        StructField("id_producteur", LongType, false),
        StructField("code_producteur", IntegerType, true),
        StructField("id_rva", IntegerType, true),
        StructField("commune", StringType, true),
        StructField("code_insee", IntegerType, true),
        StructField("type_producteur", StringType, true),
        StructField("activite", StringType, true),
        StructField("latitude", DoubleType, true),
        StructField("longitude", DoubleType, true),
        StructField("date_debut", TimestampType, false),
        StructField("date_fin", TimestampType, true),
        StructField("date_crea", TimestampType, false),
        StructField("date_modif", TimestampType, true)
      )
    )
    assertEquals(schema, result.schema)
    assertTrue(result.isEmpty)
  }

  @Test
  def createProducteurTreatmentTest(): Unit ={
    val df_partitionedIncomingProducteur: DataFrame = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/createProducteurTreatmentTest/df_partitionedIncomingProducteur.csv")

    val datePhoto: String = "20220216"
    val df_latest: DataFrame = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/createProducteurTreatmentTest/df_latest.csv")

    val result = DechetAnalysis.createProducteurTreatment(df_partitionedIncomingProducteur, datePhoto, df_latest)
    assertTrue(result.filter(col("date_modif") =!=
      Utils.timestampWithoutZone()(lit(datePhoto),lit("000000"))).isEmpty)
    assertEquals(1,result.count())
  }

  @Test
  def createBacTreatmentTest(): Unit ={
    val df_partitionedIncomingBac: DataFrame = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/createBacTreatmentTest/df_partitionedIncomingBac.csv")

    val datePhoto: String = "20220209"
    val df_latest: DataFrame = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/createBacTreatmentTest/df_latest.csv")

    val result = DechetAnalysis.createRecipientTreatment(df_partitionedIncomingBac, datePhoto, df_latest)
    assertTrue(result.filter(col("date_modif") =!=
      Utils.timestampWithoutZone()(lit(datePhoto),lit("000000"))).isEmpty)
    assertEquals(1,result.count())
  }

  @Test
  def joinLatestBacAndLatestProducteurTest(): Unit ={

    val df_latestProducteur: DataFrame = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/joinLatestBacAndLatestProducteurTest/df_latestProducteur.csv")
    df_latestProducteur.show()
    val df_latestBac: DataFrame = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/joinLatestBacAndLatestProducteurTest/df_latestBac.csv")

    val dfExpected: DataFrame = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/joinLatestBacAndLatestProducteurTest/dfExpected.csv")

    val result = DechetAnalysis.joinLatestBacAndLatestProducteur(df_latestBac, df_latestProducteur)

    assertEquals(0, result.except(dfExpected).count())

  }

  @Test
  def deletedProducteurTreatmentTest(): Unit ={
    val df_partitionedIncomingProducteur: DataFrame = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/deletedProducteurTreatmentTest/df_partitionedIncomingProducteur.csv")
    val datePhoto: String = "20220216"
    val df_latest: DataFrame = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/deletedProducteurTreatmentTest/df_latest.csv")

    val result = DechetAnalysis.deletedProducteurTreatment(df_partitionedIncomingProducteur, datePhoto, df_latest)

    assertTrue(result.filter(col("date_modif") =!=
      Utils.timestampWithoutZone()(lit(datePhoto),lit("000000"))).isEmpty)
    assertTrue(result.filter(col("date_fin") =!=
      Utils.timestampWithoutZone()(lit(datePhoto),lit("000000"))).isEmpty)
    assertEquals(1,result.count())
  }

  @Test
  def deletedRecipientTreatment(): Unit ={
    val df_partitionedIncomingBac: DataFrame = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/deletedRecipientTreatment/df_partitionedIncomingBac.csv")
    val datePhoto: String = "20220216"
    val df_latest: DataFrame = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/deletedRecipientTreatment/df_latest.csv")

    val result = DechetAnalysis.deletedRecipientTreatment(df_partitionedIncomingBac, datePhoto, df_latest)

    assertTrue(result.filter(col("date_modif") =!=
      Utils.timestampWithoutZone()(lit(datePhoto),lit("000000"))).isEmpty)
    assertTrue(result.filter(col("date_fin") =!=
      Utils.timestampWithoutZone()(lit(datePhoto),lit("000000"))).isEmpty)
    assertEquals(1,result.count())
  }

/*  @Test
  def updatedRecipientTreatment(): DataFrame = {
    val df_partitionedIncomingBac: DataFrame = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/updatedRecipientTreatment/df_partitionedIncomingBac.csv")
    val datePhoto: String = "20220216"
    val df_latest: DataFrame = spark
      .read
      .option("header", "true")
      .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
      .option("delimiter", ";")
      .load("src/test/resources/Local/services/Analysis/updatedRecipientTreatment/df_latest.csv")

    DechetAnalysis.updatedRecipientTreatment(df_latest, df_partitionedIncomingBac, datePhoto)
  }*/
}