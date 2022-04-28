package fr.rennesmetropole.services

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.tableVar
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp


object ImportDechet {

  val logger = Logger(getClass.getName)

  /**
   * Declare un dataframe via la lecture par date dans MinIO 
   *
   * @param spark : la session spark
   * @param DATE  : la date de la semaine à analyser
   * @return
   */
  def ExecuteImportDechet(spark: SparkSession, DATE: String,nameEnv :String): DataFrame = {
    readFromMinio(spark, DATE, nameEnv)
  }

  /**
   *
   * @param spark : la session spark
   * @param DATE  : date de la semaine à analyser
   * @return
   */
  def readFromMinio(spark: SparkSession, DATE: String, nameEnv:String): DataFrame = {
    /** Lecture de la donnée dans Minio */
    try {
      Utils.readData(spark, DATE, nameEnv)
    } catch {
      case e: Throwable => {
        logger.error("Erreur de chargement des fichiers depuis MinIO") 
        throw new Exception("Erreur de chargement des fichiers depuis MinIO", e)
      }
    }
  }

  def readLastestReferential(spark: SparkSession, DATE: String, nameEnv :String) : DataFrame = {
    val URL = tableVar(nameEnv, "analysed_bucket")
    try {
      val pathDf = spark
        .read
        .orc(URL+"latest")
      pathDf.show

      val readDf = spark
        .read.options(Map("header"->"true", "delimiter"->";"))
        .orc(URL+pathDf.orderBy(col("date").desc).first().getAs("path"))
      readDf.show(false)
      readDf
    } catch {
      case e : Throwable =>
        println("ERROR while reading data at : " + URL+"latest \nError stacktrace :"+ e)
        if("tableProducteur".equals(nameEnv))
          createEmptyProducteurDataFrame(spark, DATE, true)
        else if("tableRecipient".equals(nameEnv))
          createEmptyBacDataFrame(spark, DATE , true)
        else if("tableCollecte".equals(nameEnv))
          createEmptyCollecteDataFrame(spark)
        else if("tableExutoire".equals(nameEnv))
          createEmptyExutoireDataFrame(spark)
        else
          null
    }
  }

  def createEmptyProducteurDataFrame(spark: SparkSession, DATE: String, insert: Boolean = false) = {
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
        StructField("date_modif", TimestampType, true),
        StructField("year", StringType, true),
        StructField("month", StringType, true),
        StructField("day", StringType, true)
      )
    )
    var newDF:DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    if(insert) {
      val frTZ = java.time.ZoneId.of("Europe/Paris")
      val now = Timestamp.from(java.time.ZonedDateTime.now(frTZ).withNano(0).toInstant)
      val dateDebut = Timestamp.from(java.time.ZonedDateTime.of(2020,1,1,0,0,0,0,frTZ).toInstant)
      val producteurIndetermine = Seq(Row(Utils.createId("null", DATE.replaceAll("-", "")), null, null,
        "Indéterminé", null, null, "Indéterminée", null, null, dateDebut, null,
        now, null, null, null, null))
      newDF = spark.createDataFrame(spark.sparkContext.parallelize(producteurIndetermine), schema)
    }
    //newDF.show()
    newDF
  }

  def createEmptyBacDataFrame(spark: SparkSession, DATE: String, insert: Boolean = false): DataFrame = {
    val schema = StructType(
      List(
        StructField("id_bac", LongType, false),
        StructField("code_puce", StringType, true),
        StructField("code_producteur", IntegerType, true),
        StructField("categorie_recipient", StringType, true),
        StructField("type_recipient", StringType, true),
        StructField("litrage_recipient", IntegerType, true),
        StructField("type_puce", StringType, true),
        StructField("nb_collecte", FloatType, true),
        StructField("id_producteur", LongType, true),
        StructField("date_debut", TimestampType, false),
        StructField("date_fin", TimestampType, true),
        StructField("date_crea", TimestampType, false),
        StructField("date_modif", TimestampType, true),
        StructField("year", StringType, true),
        StructField("month", StringType, true),
        StructField("day", StringType, true)
      )
    )

    var newDF:DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    if(insert){
      val frTZ = java.time.ZoneId.of("Europe/Paris")
      val now = Timestamp.from(java.time.ZonedDateTime.now(frTZ).withNano(0).toInstant)
      val dateDebut = Timestamp.from(java.time.ZonedDateTime.of(2020,1,1,0,0,0,0,frTZ).toInstant)
      val bacIndetermine = Seq(Row(Utils.createId("null",DATE.replaceAll("-", "")), null, null,
        "Indéterminée", "Indéterminé", null, "Absent", null, Utils.createId("null",DATE.replaceAll("-", "")),  dateDebut, null,
        now, null, null,null,null),
        Row(Utils.createId("INCONNU",DATE.replaceAll("-", "")), "INCONNU", null,
          "Indéterminé", "Indéterminé", null, "Inconnu", null, Utils.createId("null", DATE.replaceAll("-", "")), dateDebut, null,
          now, null, null,null,null))
      newDF = spark.createDataFrame(spark.sparkContext.parallelize(bacIndetermine), schema)
    }else {

    }

   // newDF.show()
    newDF
  }

  def createEmptyCollecteDataFrame(spark: SparkSession): DataFrame = {
    val schema = StructType(
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
        StructField("Statut bac", StringType, true),
        StructField("Tournee", StringType, true),
        StructField("Immatriculation ", StringType, true)
      )
    )
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  def createEmptyExutoireDataFrame(spark: SparkSession): DataFrame = {
    val exutoires = StructType(
      List(
        StructField("code_immat", StringType, false), //Immat.
        StructField("date_mesure", StringType, true), //Date service véhic.
        StructField("code_tournee", IntegerType, true), //Description tournée
        StructField("distance", IntegerType, false), //Km réalisé
        StructField("secteur", StringType, true), //LOT
        StructField("nb_bac", IntegerType, false), //Service
        StructField("localisation_vidage", StringType, true), //Nom rech. lieu de vidage
        StructField("poids", DoubleType, true), //Nom multiples ligne
        StructField("type_vehicule", FloatType, true)//, //Clé unique Ligne ticket
        //StructField("date_crea", StringType, false),
        //StructField("date_modif", StringType, false)
      )
    )
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], exutoires)
  }

}