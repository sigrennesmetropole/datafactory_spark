package fr.rennesmetropole.services

import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.{show, tableVar, logger}
import fr.rennesmetropole.tools.Utils.log
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp


object ImportDechet {


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
        //throw new Exception("Erreur de chargement des fichiers depuis MinIO", e)
        nameEnv match {
          case "tableCollecte" =>
            createEmptyCollecteDataFrame(spark)
          case "tableProducteur" =>
            createEmptyProducteurDataFrame(spark,DATE)
          case "tableRecipient" =>
            createEmptyBacDataFrame(spark,DATE)
          case "tableExutoire" =>
            createEmptyExutoireDataFrame(spark)
        }
      }
    }
  }

  /**
   *
   * @param spark : la session spark
   * @param DATE  : date de la semaine à analyser
   * @return
   */
  def readSmartDataFromMinio(spark: SparkSession, DATE: String, nameEnv:String): DataFrame = {
    /** Lecture de la donnée dans Minio */
    try {
      Utils.readSmartData(spark, DATE, nameEnv)
    } catch {
      case e: Throwable => {
        logger.error("Erreur de chargement des fichiers depuis MinIO")
        throw new Exception("Erreur de chargement des fichiers depuis MinIO", e)
      }
    }
  }

  def readLastestReferential(spark: SparkSession, DATE: String, nameEnv :String) : DataFrame = {
    val URL = tableVar(nameEnv, "analysed_bucket")
    if(Utils.envVar("TEST_MODE") == "False") {
    try {
      log("Lecture data latest...")
      val pathDf = spark
        .read
        .orc(URL+"latest")
      show(pathDf,"readLastestReferential")

      val readDf = spark
        .read.options(Map("header"->"true", "delimiter"->";"))
        .orc(URL+pathDf.orderBy(col("date").desc).first().getAs("path"))
      readDf.show()
      readDf
    } catch {
      case e : Throwable =>
        log("ERROR while reading data at : " + URL+"latest \nError stacktrace :"+ e)
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
  else{
    val URL = tableVar(nameEnv, "test_bucket_ref")
    if(DATE == "WrongDate") {
      if("tableProducteur".equals(nameEnv))
        createEmptyProducteurDataFrame(spark, DATE)
      else if("tableRecipient".equals(nameEnv))
        createEmptyBacDataFrame(spark, DATE)
      else if("tableCollecte".equals(nameEnv))
        createEmptyCollecteDataFrame(spark)
      else if("tableExutoire".equals(nameEnv))
        createEmptyExutoireDataFrame(spark)
      else
        null
    }else {
      try {
        log("LECTURE LATEST REF")
         if("tableProducteur".equals(nameEnv)) {
          spark.read.format("orc").load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_prod/orc/ref_prod.orc")
        }
        else if("tableRecipient".equals(nameEnv)) {
          log("LECTURE LATEST REF BAC")
            spark.read.format("orc").load("src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/latest_ref_bac/orc/ref_bac.orc")
        }else
        null


      } catch {
        case e : Throwable =>
        log("ERROR while reading data at : " + URL+"latest \nError stacktrace :"+ e)
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
    }
  }

  def readDatedReferential(spark: SparkSession, DATE: String, nameEnv :String) : DataFrame = {
    val URL = tableVar(nameEnv, "analysed_bucket")
    val postURL = Utils.date2URL(DATE)+"orc"
    try {
      val pathDf = spark
        .read
        .orc(URL+postURL)
      show(pathDf)
      pathDf
    } catch {
      case e : Throwable =>
        log("ERROR while reading data at : " + URL+postURL+" \nError stacktrace :"+ e)
        log("Initialisation de la table...")
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

  def createEmptyProducteurDataFrame(spark: SparkSession, DATE: String, insert: Boolean = false):DataFrame = {
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
      var now = Timestamp.from(java.time.ZonedDateTime.now(frTZ).withNano(0).toInstant)
      if(Utils.envVar("TEST_MODE") != "False"){
        now = Timestamp.from(java.time.ZonedDateTime.of(2020,1,1,12,0,0,0,frTZ).toInstant)
      }
      val dateDebut = Timestamp.from(java.time.ZonedDateTime.of(2020,1,1,0,0,0,0,frTZ).toInstant)
      val producteurIndetermine = Seq(Row(Utils.createId("null", DATE.replaceAll("-", "")), null, null,
        "Indetermine", null, null, "Indeterminee", null, null, dateDebut, null,
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
        StructField("nb_collecte", DoubleType, true),
        StructField("date_debut", TimestampType, false),
        StructField("date_fin", TimestampType, true),
        StructField("date_crea", TimestampType, false),
        StructField("date_modif", TimestampType, true),
        StructField("year", StringType, true),
        StructField("month", StringType, true),
        StructField("day", StringType, true),
        StructField("id_producteur", LongType, true)
      )
    )

    var newDF:DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    if(insert){
      val frTZ = java.time.ZoneId.of("Europe/Paris")
      var now = Timestamp.from(java.time.ZonedDateTime.now(frTZ).withNano(0).toInstant)
      if(Utils.envVar("TEST_MODE") != "False"){
        now = Timestamp.from(java.time.ZonedDateTime.of(2020,1,1,12,0,0,0,frTZ).toInstant)
      }
      val dateDebut = Timestamp.from(java.time.ZonedDateTime.of(2020,1,1,0,0,0,0,frTZ).toInstant)

      val bacIndetermine = Seq(Row(Utils.createId("null",DATE.replaceAll("-", "")), "null", 0,
        "Indeterminee", "Indetermine", 0, "Absent", null,  dateDebut, null,
        now, null, null,null,null, Utils.createId("null",DATE.replaceAll("-", ""))),

        Row(Utils.createId("INCONNU",DATE.replaceAll("-", "")), "INCONNU", 0,
          "Indetermine", "Indetermine", 0, "Inconnu", null, dateDebut, null,
          now, null, null,null,null, null, Utils.createId("null", DATE.replaceAll("-", ""))))
      newDF = spark.createDataFrame(spark.sparkContext.parallelize(bacIndetermine), schema)
            show(newDF,"createEmptyBacDataFrame creation bacIndetermine")
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
        StructField("Immatriculation ", StringType, true),
        StructField("Tournee", StringType, true)
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
   val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], exutoires)
    df
  }

  def verif(spark: SparkSession, df_toVerif:DataFrame ,nameEnv :String) : (DataFrame,String) ={
    import spark.implicits._
    val list_name = Utils.getListName(nameEnv)
    val df_prepared_sort = df_toVerif.select(list_name.head,list_name.tail:_*)
    var df_doublon_garder = spark.emptyDataFrame

    var exception = "\n"
    //verification de l'intégrité des données
    try {
      nameEnv match {
        case "tableProducteur" => {
          //recuperation des codes qui sont en doublons
          val verif_codeProducteur = df_prepared_sort.groupBy("code_producteur").count().filter("count > 1")

          if (!verif_codeProducteur.isEmpty) {
            //recuperation du dataframe contenant les doublons
            val df_doublons = df_toVerif.filter(verif_codeProducteur("code_producteur").isInCollection(verif_codeProducteur.rdd.map(row => row(0)).collect().toList))
            val w2 = Window.partitionBy("code_producteur").orderBy(col("code_producteur"))
            //recuperation d'une seule ligne par code qui est en doublon
            val df_a_garder = df_doublons.withColumn("row", row_number.over(w2))
              .where($"row" === 1).drop("row")
            //creation du dataframe total avec une seul ligne par code
            df_doublon_garder = df_toVerif.filter(!verif_codeProducteur("code_producteur").isInCollection(verif_codeProducteur.rdd.map(row => row(0)).collect().toList))
              .unionByName(df_a_garder)
            //creation des logs pour les alertes
            exception = exception + "- Referentiel PRODUCTEUR invalide, deux codes producteur identique trouve. Liste des codes producteurs corrompu :\n"
            //Extraction des doublons pour les l'alerte
            val doublons_codeProducteur = verif_codeProducteur.collect().map(_.get(0)).mkString(" ")
            exception = exception + doublons_codeProducteur + "\n"
            df_doublons.orderBy(col("code_producteur")).collect().foreach(row =>
              exception = exception + row.toSeq.toString().replace("WrappedArray","") + "\n"
            )
            exception = exception + "\nLigne retenu :\n" + df_a_garder.columns.mkString(" - ") +"\n"
            df_a_garder.collect().foreach(row =>
              exception = exception + row.toSeq.toString().replace("WrappedArray","") + "\n"
            )
          }else {
            log("pas de doublons prod")
          }

        }
        case "tableRecipient" => {
          //recuperation des codes qui sont en doublons
          val verif_codeBac = df_prepared_sort.groupBy("code_puce").count().filter("count > 1")
          if (!verif_codeBac.isEmpty) {
            //recuperation du dataframe contenant les doublons
            val df_doublons = df_toVerif.filter(verif_codeBac("code_puce").isInCollection(verif_codeBac.rdd.map(row => row(0)).collect().toList))
            val w2 = Window.partitionBy("code_puce").orderBy(col("code_puce"))
            //recuperation d'une seule ligne par code qui est en doublon
            val df_a_garder = df_doublons.withColumn("row", row_number.over(w2))
              .where($"row" === 1).drop("row")
            df_doublon_garder = df_toVerif.filter(!verif_codeBac("code_puce").isInCollection(verif_codeBac.rdd.map(row => row(0)).collect().toList))
              .unionByName(df_a_garder)
            //creation des logs pour les alertes
            exception = exception + "- Referentiel BAC invalide, deux codes puce identique trouve. Liste des codes puces corrompu :\n "
            //Extraction des doublons pour les l'alerte
            val doublons_codePuces = verif_codeBac.collect().map(_.getString(0)).mkString(" ")
            exception = exception + doublons_codePuces + "\n"
            df_doublons.orderBy(col("code_puce")).collect().foreach(row =>
              exception = exception + row.toSeq.toString().replace("WrappedArray","") + "\n"
            )
            exception = exception + "\nLigne retenu :\n" + df_a_garder.columns.mkString(" - ") +"\n"
            df_a_garder.collect().foreach(row =>
              exception = exception + row.toSeq.toString().replace("WrappedArray","") + "\n"
            )

          }
          else {
            log("pas de doublons bac")
          }
        }
      }
      if (exception != "\n") {
        logger.error(exception)
        (df_doublon_garder, exception)
      }else {
        (df_toVerif, "")
      }

    }
      catch{
        case e: Throwable => {
          logger.error(exception)
          throw new Exception("erreur lors de la verif de " + nameEnv, e )
        }

      }
  }

  def importRedressement(spark: SparkSession,DATE: String,nameEnv:String) : (DataFrame,DataFrame,String) = {
    val date = DATE.split("-") // date devant etre sous la forme yyyy-mm-dd
    val year = date(0);
    var month = date(1);
    var monthSupport = date(1);
    var namePathSupport = "analysed_bucket"
    var namePathRawData = "redressed_bucket"
    var type_support = "smart_orc"
    if(year.matches("2022")){
      monthSupport = "01";
      type_support = "csv"
    }
    val year_ref = year.toInt-1
    val dateRef = year_ref+"-"+month
    val dateSupport = year_ref+"-"+monthSupport
    log("DATE Donne support mesure :" + dateRef)
    log("DATE Donne support minio :" + dateSupport)
    // return un dataframe contenant les donnees qui vont permettre de corrigé les donnees qui vienne d'etre integrer

    if (Utils.envVar("TEST_MODE") == "True"){
      namePathSupport = "analysed_bucket_test"
      namePathRawData ="analysed_bucket_test"
      val dateRef = "2021-01"
      (Utils.readDataPath(spark, dateSupport, nameEnv,namePathSupport,"csv"),Utils.readDataPath(spark,DATE,nameEnv,namePathRawData,"csv"),dateRef)
    }
    else {
      (Utils.readDataPath(spark, dateSupport, nameEnv, namePathSupport, type_support), Utils.readDataPath(spark, DATE, nameEnv, namePathRawData, "orc"), dateRef)
    }
    }
}