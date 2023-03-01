package fr.rennesmetropole.tools

import com.typesafe.config.{Config, ConfigFactory}
import fr.rennesmetropole.services.DechetAnalysis.typeFrequence
import fr.rennesmetropole.services.ImportDechet.{createEmptyBacDataFrame, createEmptyCollecteDataFrame, createEmptyExutoireDataFrame, createEmptyProducteurDataFrame}
import org.apache.commons.lang3.StringUtils.stripAccents
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.{Connection, DriverManager, SQLException, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util
import java.util.Properties
import org.apache.logging.log4j.{Level, LogManager}

object Utils {

  val logger = LogManager.getLogger(getClass.getName)
  val USER_LOG = Level.forName("DATA FACTORY", 200);
  var config = ConfigFactory.load()

def log(msg:Any):Unit ={
  logger.log(USER_LOG, msg)
}
  /**
   *
   * @param name : nom de la configuration
   * @param no_error
   * @param default
   * @return
   */
  def envVar(name: String, no_error: Boolean = false, default: String = ""): String = {
    var value: String = config.getString("env." + name)
    if (value == null && (no_error == null || no_error == false)) {
      throw new Exception("Env var '" + name + "' must be defined")
    }
    if (value == null && default != null) {
      default
    }
    value
  }

 /**
    *
    * @param name : nom de la configuration
    * @param no_error
    * @param default
    * @return
    */
    def tableVar(nameEnv:String, name: String, no_error: Boolean = false, default: String = ""): String = {
      var value: String = config.getString(nameEnv + "." + name)
      if (value == null && (no_error == null || no_error == false)) {
        throw new Exception(nameEnv+" var '" + name + "' must be defined")
      }
      if (value == null && default != null) {
        default
      }
      value
    }


 /**
    *
    * @param spark  : la session spark
    * @param url    : url complète du stockage
    * @param schema : le schéma des données à lire
    * @return Dataframe
    */
    def readData(spark: SparkSession, DATE: String, nameEnv:String ): DataFrame = {

      if (Utils.envVar("TEST_MODE") == "False") {
        val URL = tableVar(nameEnv, "out_bucket")
        /* calcul du chemin pour lire les données sur minio */
        val postURL = date2URL(DATE)
        log("URL de lecture sur Minio : " + URL + postURL)

        try {
          spark
            .read
            .orc(URL + postURL)

        } catch {
          case e: Throwable =>
            log("ERROR while reading data at : " + URL + postURL + " \nError stacktrace :" + e)
            if ("tableProducteur".equals(nameEnv))
              createEmptyProducteurDataFrame(spark, DATE)
            else if ("tableRecipient".equals(nameEnv))
              createEmptyBacDataFrame(spark, DATE)
            else if ("tableCollecte".equals(nameEnv))
              createEmptyCollecteDataFrame(spark)
            else if ("tableExutoire".equals(nameEnv))
              createEmptyExutoireDataFrame(spark)
            else {
              val schemaTest = StructType(
                List(
                  StructField("id_test", StringType, false)
                )
              )
              spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schemaTest)
            }
        }


        // else... si nous somme en mode TEST
      } else {
        val URL = tableVar(nameEnv, "test_bucket")
        val postURL = date2URL(DATE)
        log("URL LECTURE TEST : " + URL + postURL)
        if (DATE == "WrongDate") {
          if ("tableProducteur".equals(nameEnv))
            createEmptyProducteurDataFrame(spark, DATE)
          else if ("tableRecipient".equals(nameEnv))
            createEmptyBacDataFrame(spark, DATE)
          else if ("tableCollecte".equals(nameEnv))
            createEmptyCollecteDataFrame(spark)
          else if ("tableExutoire".equals(nameEnv))
            createEmptyExutoireDataFrame(spark)
          else {
            val schemaTest = StructType(
              List(
                StructField("id_test", StringType, false)
              )
            )
            spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schemaTest)
          }
        } else {
          spark
            .read
            .option("header", "true")
            .format("csv")
            .option("delimiter", ";")
            .load(URL + postURL)

        }
      }
    }

  /**
   *
   * @param spark  : la session spark
   * @param url    : url complète du stockage
   * @param schema : le schéma des données à lire
   * @return Dataframe
   */
  def readDataPath(spark: SparkSession, DATE: String, nameEnv:String ,namePath:String,typeFile:String): DataFrame = {

    if (Utils.envVar("TEST_MODE") == "False") {
      val URL = tableVar(nameEnv, namePath)
      /* calcul du chemin pour lire les données sur minio */
      val postURL = date2URL(DATE)
      log("URL de lecture sur Minio : " + URL + postURL)
      try {
        typeFile match {
          case "csv" =>
            spark
              .read
              .option("header", "true")
              .option("compression", "gzip")
              .format("csv")
              .option("delimiter", ";")
              .load(URL+postURL)
          case "orc" =>
            spark
              .read
              .orc(URL + postURL)
          case "smart_orc" =>
             val fromFolder = {
              println("Reading smart data from : " + URL + postURL)
              new Path(URL + postURL)
            }
            val conf = spark.sparkContext.hadoopConfiguration
            val logfiles = fromFolder.getFileSystem(conf)
              .listFiles(fromFolder, true)
            var files = Seq[String]()
            while (logfiles.hasNext) {
              // one can filter here some specific files
              files = files :+ logfiles.next().getPath().toString
            }
            files = files.filter(s => s.contains(".orc"))
            //affiche le nombre de fichier trouvé
            println("number of files read : " + files.length)
            //affiche le path de tout les fichier trouvé
            println(files.mkString("-"))
            spark
              .read
              .orc(files:_*)
        }
      } catch {
        case e: Throwable =>
          log("ERROR while reading data at : " + URL + postURL + " \nError stacktrace :" + e)
          if ("tableProducteur".equals(nameEnv))
            createEmptyProducteurDataFrame(spark, DATE)
          else if ("tableRecipient".equals(nameEnv))
            createEmptyBacDataFrame(spark, DATE)
          else if ("tableCollecte".equals(nameEnv))
            createEmptyCollecteDataFrame(spark)
          else if ("tableExutoire".equals(nameEnv))
            createEmptyExutoireDataFrame(spark)
          else {
            val schemaTest = StructType(
              List(
                StructField("id_test", StringType, false)
              )
            )
            spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schemaTest)
          }
      }


      // else... si nous somme en mode TEST
    } else {
      val URL = tableVar(nameEnv, namePath)

      val postURL = date2URL(DATE)
      log("URL LECTURE TEST : " + URL + postURL)
      if (DATE == "WrongDate") {
        if ("tableProducteur".equals(nameEnv))
          createEmptyProducteurDataFrame(spark, DATE)
        else if ("tableRecipient".equals(nameEnv))
          createEmptyBacDataFrame(spark, DATE)
        else if ("tableCollecte".equals(nameEnv))
          createEmptyCollecteDataFrame(spark)
        else if ("tableExutoire".equals(nameEnv))
          createEmptyExutoireDataFrame(spark)
        else {
          val schemaTest = StructType(
            List(
              StructField("id_test", StringType, false)
            )
          )
          spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schemaTest)
        }
      } else {
        typeFile match {
          case "csv" =>
            spark
              .read
              .option("header", "true")
              .option("compression", "gzip")
              .format("csv")
              .option("delimiter", ";")
              .load(URL+postURL)
          case "orc" =>
            spark
              .read
              .orc(URL + postURL)
        }
      }
    }
  }

  def readSmartData(spark: SparkSession, DATE: String, nameEnv:String ): DataFrame = {
    val URL = tableVar(nameEnv,"analysed_bucket")
    /* calcul du chemin pour lire les données sur minio */
    val postURL = date2URL(DATE)
    log("URL de lecture sur Minio : " + URL + postURL+"orc/")
    try {
      spark
        .read
        .orc(URL + postURL+"orc/")
    } catch {
      case e : Throwable =>
        log("ERROR while reading data at : " + URL+postURL +"orc/"+ " \nError stacktrace :"+ e)
        spark.emptyDataFrame
      }
    }

    /**
    * Permet de prendre une date en input et de ressortir la version partitionne
    *
    * @param DATE date sous forme  yyyy-mm-dd
    * @return string sous la forme yyyy/mm/ss ou ss est le numero de la semaine du mois courant
    */
    def date2URL(DATE: String): String = {
      val date = DATE.split("-") // donne la date sous forme yyyy-mm-dd
      var postURL=""
      val year = date(0);
      val month = date(1);
      if(date.length >=3){
        val day = date(2);
        postURL = "year=" + year + "/month=" + month + "/day=" + day + "/";
      }else{
        postURL = "year=" + year + "/month=" + month
      }
      postURL


    }

  /**
   *
   * @param spark     : La session spark
   * @param pgUrl     : l'url de postgresql
   * @param dfToWrite : le dataframe à écrire dans postgres
   * @param pgTable   : le nom de la table dans postres
   */
  def postgresPersist(spark: SparkSession, pgUrl: String, dfToWrite: DataFrame, pgTable: String,DATE : String): Unit = {

    log("Proceed to write data in table " + pgTable)

    val connectionProps = new Properties()
    connectionProps.setProperty("driver", "org.postgresql.Driver")
    connectionProps.setProperty("user", Utils.envVar("POSTGRES_ACCESS_KEY"))
    connectionProps.setProperty("password", Utils.envVar("POSTGRES_SECRET_KEY"))

    val nb = delete_partition(pgTable,pgUrl,DATE)
    log("fonction suppression fini ")

    //Passing in the URL, table in which data will be written and relevant connection properties
    dfToWrite.write.mode(SaveMode.Append).jdbc(pgUrl, pgTable, connectionProps)
  }

   def postgresPersistOverwrite(spark: SparkSession, pgUrl: String, dfToWrite: DataFrame, pgTable: String): Unit = {

    log("Processus de remplacement en cours sur toute les donnes de " + pgTable)

    val connectionProps = new Properties()
    connectionProps.setProperty("driver", "org.postgresql.Driver")
    connectionProps.setProperty("user", Utils.envVar("POSTGRES_ACCESS_KEY"))
    connectionProps.setProperty("password", Utils.envVar("POSTGRES_SECRET_KEY"))

    val nb = delete_table(pgTable,pgUrl)
    log("fonction suppression fini ")

    //Passing in the URL, table in which data will be written and relevant connection properties
    dfToWrite.write.mode(SaveMode.Append).jdbc(pgUrl, pgTable, connectionProps)
    log("Donnee ecrite dans la base de donnees postgres")
  }

  /** 
    * Supprime la partition par rapport à la table et la date en entrée
    * @param table_name : Nom de la table
    * @param pgUrl      : l'url de postgresql
    * @param DATE       : date sous forme  yyyy-mm-dd 
    * @return 
    */
  def delete_partition(table_name:String, pgUrl: String, DATE:String){
    val arrayDate = DATE.split("-")
    val driverClass = "org.postgresql.Driver"
    var connObj:Connection = null
    var number_of_rows_deleted:Int = 0
    try{
      Class.forName(driverClass);
      connObj = DriverManager.getConnection(pgUrl,  Utils.envVar("POSTGRES_ACCESS_KEY"), Utils.envVar("POSTGRES_SECRET_KEY"));
      var stringDelete = "DELETE FROM "+ table_name +" WHERE year='"+ arrayDate(0) +"' AND month='"+ arrayDate(1)+"'"
      if(arrayDate.length == 3) stringDelete += " AND  day='"+ arrayDate(2)+"'"
      val statement = connObj.prepareStatement(stringDelete)
      try{
          number_of_rows_deleted = statement.executeUpdate();
      }
      finally{
          statement.close();
          log(number_of_rows_deleted + " rows deleted.")
      }
    }
    catch {
      case e:SQLException => log("error lors le la suppression des valeurs partitionnées"); e.printStackTrace();
    }
    finally{
      connObj.close();
    }
  
  }

       /** 
    * Supprime la partition par rapport à la table et la date en entrée
    * @param table_name : Nom de la table
    * @param pgUrl      : l'url de postgresql
    * @param DATE       : date sous forme  yyyy-mm-dd 
    * @return 
    */
  def delete_table(table_name:String, pgUrl: String){
    val driverClass = "org.postgresql.Driver"
    var connObj:Connection = null
    var number_of_rows_deleted:Int = 0
    try{
      Class.forName(driverClass);
      connObj = DriverManager.getConnection(pgUrl,  Utils.envVar("POSTGRES_ACCESS_KEY"), Utils.envVar("POSTGRES_SECRET_KEY"));
      var stringDelete = "TRUNCATE TABLE "+ table_name + " CASCADE"
      val statement = connObj.prepareStatement(stringDelete)
      try{
          number_of_rows_deleted = statement.executeUpdate();
      }
      finally{
          statement.close();
          log(number_of_rows_deleted + " rows deleted.")
      }
    }
    catch {
      case e:SQLException => log("error lors le la suppression de la table " + table_name + " : "); e.printStackTrace();
    }
    finally{
      connObj.close();
    }
  
  }

    /**
  * Manipule les valeurs des colonnes du dataframe :
  * Recupération de Year, Month et Week + Création de la colonne technical-key (par concaténation de id, start_time, year, month et week)
  * @param df le dataframe à traiter
  * @return le daframe en entré plus une colonne qui va servir de partitionnement
  */
  def dfToPartitionedDf(df : DataFrame, DATE :String) : DataFrame = {
    val arrayDate = DATE.split("-")
    val df2 = df.withColumn("year",lit(arrayDate(0))).withColumn("month",lit(arrayDate(1))).withColumn("day",lit(arrayDate(2)))
    //df2.withColumn("id_bac", concat_ws("_",col("id_bac"), col("year"), col("month"), col("day")))
    df2
  }

   def writeToS3(spark: SparkSession, df_toWrite: DataFrame, nameEnv :String): Unit = {
     writeToS3(spark, df_toWrite, nameEnv, java.time.LocalDate.now.toString)
    }

  def writeToS3(spark: SparkSession, df_toWrite: DataFrame, nameEnv :String, DATE :String): Unit = {
    val postURL = date2URL(DATE)
    log("Write to s3 to " + Utils.tableVar(nameEnv,"analysed_bucket") + postURL)

    df_toWrite.repartition(2)   // nécessaire pour écrire le DF dans un seul csv, mais pPeut poser problème si le DF est trop gros
      .write.options(Map("header"->"true", "delimiter"->";","compression"->"gzip"))
      .mode(SaveMode.Append)
      .csv(Utils.tableVar(nameEnv,"analysed_bucket") + postURL)
    log("Ecritures des fichiers csv fait")
    df_toWrite.repartition(2)   // nécessaire pour écrire le DF dans un seul csv, mais pPeut poser problème si le DF est trop gros
      .write
      .mode(SaveMode.Append)
      .orc(Utils.tableVar(nameEnv,"analysed_bucket") + postURL+"orc/")
    log("Ecritures des fichiers orc fait")

    if("tableProducteur".equals(nameEnv) || "tableRecipient".equals(nameEnv) ) {

      val schema = StructType(
        List(
          StructField("date", TimestampType, false),
          StructField("path", StringType, true)
        )
      )
      val latestPath = Seq(Row(Timestamp.valueOf(Utils.dateTimestamp(java.time.format.DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss"))),postURL+"orc/"))
      val latestDF = spark.createDataFrame(spark.sparkContext.parallelize(latestPath), schema)
      latestDF.repartition(1)   // nécessaire pour écrire le DF dans un seul csv, mais pPeut poser problème si le DF est trop gros
        .write.options(Map("header"->"true", "delimiter"->";"))
        .mode(SaveMode.Append)
        .format("orc")
        .save(Utils.tableVar(nameEnv,"analysed_bucket") + "latest")
    }

    log("Write to s3 Done")
  }

  def writeToBeRedressedToS3(spark: SparkSession, df_toWrite: DataFrame, nameEnv: String, DATE: String): Unit={
    val postURL = date2URL(DATE)
    log("Write to s3 to " + Utils.tableVar(nameEnv, "redressed_bucket") + postURL)

    df_toWrite.repartition(1) // nécessaire pour écrire le DF dans un seul csv, mais pPeut poser problème si le DF est trop gros
      .write
      .mode(SaveMode.Append)
      .orc(Utils.tableVar(nameEnv, "redressed_bucket") + postURL )
  }
    def writeToS3ForPatchOrc(spark: SparkSession, df_toWrite: DataFrame, nameEnv :String, DATE :String): Unit = {
    val postURL = date2URL(DATE)
    log("Write to s3 to " + Utils.tableVar(nameEnv,"analysed_bucket") + postURL)

    df_toWrite.repartition(1)   // nécessaire pour écrire le DF dans un seul csv, mais pPeut poser problème si le DF est trop gros
      .write.options(Map("header"->"true", "delimiter"->";","compression"->"gzip"))
      .mode(SaveMode.Append)
      .csv(Utils.tableVar(nameEnv,"analysed_bucket") + postURL+"patch/")

    if("tableRecipient".equals(nameEnv) ) {
      log("Application, du patch en cours...")
      df_toWrite.repartition(1)   // nécessaire pour écrire le DF dans un seul csv, mais pPeut poser problème si le DF est trop gros
        .write
        .mode(SaveMode.Append)
        .orc(Utils.tableVar(nameEnv,"analysed_bucket") + postURL+"orc/patch/")
    }

    log("Write to s3 Done")
  }


    def dateTimestamp(format:DateTimeFormatter):String ={
      val frTZ = java.time.ZoneId.of("Europe/Paris")
      java.time.ZonedDateTime.now(frTZ).format(format)//.toString.split(":")(1)

    }
    def dateTimestampWithZone(format:String):String ={
      val frTZ = java.time.ZoneId.of("Europe/Paris")
      java.time.ZonedDateTime.now(frTZ).format(java.time.format.DateTimeFormatter.ofPattern(format)) + java.time.ZonedDateTime.now(frTZ).getOffset//.toString.split(":")(1)

    }


    def dateToTimestamp(date:String): String = {
      val dateSplit = date.split("-")
      val time = date + " 00:" + dateSplit(1) + ":" + dateSplit(2)
      time

    }

    def createId(s:String*): Long = {
     hash(s.mkString(""))
    }

  def hash(u: String): Long = {
    val a = scala.util.hashing.MurmurHash3.stringHash(u)
    val b = scala.util.hashing.MurmurHash3.stringHash(u.reverse.toString)
    // shift a 32 bits to the left, leaving 32 lower bits zeroed
    // mask b with "1111...1" (32 bits)
    // bitwise "OR" between both leaving a 64 bit hash of the string using Murmur32 on each portion
    val c: Long = a.toLong << 32 | (b & 0xffffffffL)
    c
  }
    def typePuce( code:String): String = {
      if(code == null){
        "Inconnu"
      }else {
        "Connu"
      }
    }

    /** UDF type de puce => inconnu / connu / absent ?*/
    def typePuceUDF(): UserDefinedFunction = {
      val typeDePuce = (code_puce:String) =>{typePuce(code_puce) }
      udf(typeDePuce)
    }

    /** UDF choix de la frequence a mettre dans la colonne nb_collecte */
    def typedeFreqUdf(): UserDefinedFunction = {
      val typeDeFreq = (type_puce: String, frequence_om: Integer, frequence_cs: Float) => {
        typeFrequence(type_puce, frequence_om, frequence_cs)
      }
      udf(typeDeFreq)
    }

    def idBacUdf(): UserDefinedFunction = {
      val idBac = (code_puce: String, datePhoto: String) => {
        Utils.createId(code_puce, datePhoto)
      }
      udf(idBac)
    }

    def timestampWithoutZone() :UserDefinedFunction = {
      val merge = (date:String,heure:String) =>{Timestamp.valueOf(mergeToTimestampWithoutTimezone(date,heure)) }
      udf(merge)
    }

    def timestampWithZoneUdf() :UserDefinedFunction = {
      
      val merge = (date:String,heure:String) =>{
         var heure_leftpadded = heure
        while(heure_leftpadded.length < 6){
          heure_leftpadded = "0"+heure_leftpadded
        }
        ZonedDateTime.of(Integer.valueOf(date.substring(0,4)), Integer.valueOf(date.substring(4,6)), Integer.valueOf(date.substring(6,8)),
          Integer.valueOf(heure_leftpadded.substring(0,2)), Integer.valueOf(heure_leftpadded.substring(2,4)), Integer.valueOf(heure_leftpadded.substring(4,6)),
          0, ZoneId.of("Europe/Paris")).format(DateTimeFormatter.ISO_INSTANT)
      }
      udf(merge)
    }


    def mergeToTimestampWithoutTimezone(date:String,heure:String): String = {
      try{
        var heure_leftpadded = heure
        while(heure_leftpadded.length < 6){
          heure_leftpadded = "0"+heure_leftpadded
        }
        val date_ts = date.substring(0,4)+ "-" +date.substring(4,6)+ "-" +date.substring(6,8)
        val heure_ts = heure_leftpadded.substring(0,2)+ ":" +heure_leftpadded.substring(2,4)+ ":" +heure_leftpadded.substring(4,6)
        date_ts + " " + heure_ts
      }catch {
        case e: Throwable => {
          logger.error("Echec de mergeToTimestamp  for date: " + date + " heure: "+heure +" trace :" + e)
          "0000-00-00 00:00:00"
        }
      }
    }

  /*********    UDF   *********/

  def mergeToTimestamp(date:String,heure:String): String = {
    try{
      val frTZ = java.time.ZoneId.of("Europe/Paris")
      mergeToTimestampWithoutTimezone(date, heure)
    }catch {
      case e: Throwable => {
        logger.error("Echec de mergeToTimestamp  for date: " + date + " heure: "+heure +" trace :" + e)
        return "0000-00-00 00:00:00+01"

      }
    }
  }

  def idProducteurUDF(): UserDefinedFunction = {
    val idProducteur = (code_producteur:String, datePhoto:String) =>{Utils.createId(code_producteur,datePhoto) }
    udf(idProducteur)
  }

  def type_flux_UDF(mode:String): UserDefinedFunction = {
    mode match {
      case "pre_flux" =>
        val type_flux = (categorie_recipient:String,code_tournee:String,code_immat:String) =>{Utils.type_flux_pre_flux(categorie_recipient,code_tournee,code_immat) }
        udf(type_flux)
      case "intra_flux" =>
        val type_flux = (type_flux: String, categorie_recipient: String, date_mesure:String, min: String ,max: String) => {Utils.type_flux_intra_flux(type_flux, categorie_recipient,date_mesure, min,max) }
        udf(type_flux)
      case "post_flux" =>
        val type_flux = (type_flux:String,code_immat:String,categorie_recipient_moyen:String) =>{Utils.type_flux_post_flux(type_flux,code_immat,categorie_recipient_moyen) }
        udf(type_flux)
    }
  }

  def type_flux_pre_flux(categorie_recipient:String,code_tournee:String,code_immat:String):String={
    (stripAccents(categorie_recipient),code_tournee,code_immat) match {
      case (null,null,null) =>
        "Inconnu"

      case (x,_,_) if x!=null && x.contains("Bacs ordures menageres") =>
        "OM"
      case (x,_,_) if x!=null && x.contains("Bacs collecte selective") =>
        "CS"
      case (x,_,_) if x!=null && x.contains("Bacs verre") =>
        "Verre"
      case (x,_,_) if x!=null && x.contains("Bacs biodechets") =>
        "Biodechets"
      case (_,y,_) if y!=null && y.endsWith("OM") =>
        "OM"
      case (_,y,_) if y!=null && y.endsWith("CS") =>
        "CS"
      case (_,y,_) if y!=null && y.endsWith("VE") =>
        "Verre"
      case (_,y,_) if y!=null && y.endsWith("BIO") =>
        "Biodechets"
      case (x,_,_) if (x!=null && (x.contains("Bacs carton") || x.contains("Bacs papier") || x.contains("Composteurs"))) =>
        "Autres"
      case (null, _, code_immat) if (code_immat != null) =>
        "Inconnu_connu"
      case _ =>
        "Inconnu"
    }
  }

  def type_flux_intra_flux(type_flux: String, categorie_recipient: String, date_mesure:String, min: String ,max: String): String = {
    (stripAccents(type_flux), categorie_recipient, date_mesure, min, max) match {
      case ("Inconnu_connu", categorie_recipient, date_mesure,min,max) if categorie_recipient != null &&
        categorie_recipient.contains("Bacs ordures menageres") && min<date_mesure && date_mesure<max =>
        "OM"
      case ("Inconnu_connu", categorie_recipient, date_mesure,min,max) if categorie_recipient != null &&
        categorie_recipient.contains("Bacs collecte selective") && min<date_mesure && date_mesure<max =>
        "CS"
      case ("Inconnu_connu", categorie_recipient, date_mesure,min,max) if categorie_recipient != null &&
        categorie_recipient.contains("Bacs verre") && min<date_mesure && date_mesure<max =>
        "Verre"
      case ("Inconnu_connu", categorie_recipient, date_mesure,min,max) if categorie_recipient != null &&
        categorie_recipient.contains("Bacs biodechets") && min<date_mesure && date_mesure<max =>
        "Biodechets"
      case ("Inconnu_connu", _, _, _, _) =>
        "Inconnu"
      case (type_flux,_,_,_,_) =>
        type_flux
    }
  }
  def type_flux_post_flux(type_flux:String,code_immat:String,categorie_recipient_moyen:String):String={
    (stripAccents(type_flux),code_immat,categorie_recipient_moyen) match {
      case (x,_,z) if x.matches("Inconnu_connu|Inconnu") =>
        if(z==null) {
          "Inconnu"
        }else {
          z
        }
      case (x,_,_) if x.matches("Bacs ordures menageres|OM") =>
        "OM"
      case (x,_,_) if x.matches("Bacs collecte selective|CS") =>
        "CS"
      case (x,_,_) if x.matches("Bacs verre|VE|Verre") =>
        "Verre"
      case (x,_,_) if x.matches("Bacs biodechets|BIO|Biodechets") =>
        "Biodechets"
      case (x,_,_) if (x.matches("Bacs carton|Bacs papier|Composteurs")) =>
        "Autres"
      case _ =>
        "Inconnu"
    }
  }

  def redressementUDF():UserDefinedFunction = {
    val redressement = (code_tournee:String, poids:String,typeFlux:String) =>{Utils.redressementCollecte(code_tournee,poids,typeFlux) }
    udf(redressement)
  }

  def redressementCollecte(code_tournee:String,poids:String,typeFlux:String):String={
    (typeFlux,poids.toDouble) match {
      case (null,poids) =>
        if (0 < poids && poids < 250) {
          poids.toString
        } else {
          "0.0"
        }
      case ("OM",poids) =>
        if(0<poids && poids<120){
          poids.toString
        }else {
          "0.0"
        }
      case ("CS",poids) =>
        if(0<poids && poids<40){
          poids.toString
        }else {
          "0.0"
        }
      case ("Verre",poids) =>
        if(0<poids && poids<240){
          poids.toString
        }else {
          "0.0"
        }
      case (x,poids) if (x.contains("Biodechets") || x.contains("Inconnu") || x.contains("Autres"))=>
        if(0<poids && poids<250){
          poids.toString
        }else {
          "0.0"
        }
      case _ =>
        "0.0"
    }
  }

  def redressementCorrectionInvalideUDF(mapMoyenneBacRattache:scala.collection.Map[String,Double],mapMoyenneSansBacRattache:scala.collection.Map[String,Double],mapMoyenneBacInconnu:Double):UserDefinedFunction = {
    val redressement = (typeFlux:String,litrage_recipient:String,poids_corr:String) =>{Utils.redressementCorrectionInvalide(typeFlux,litrage_recipient,poids_corr,mapMoyenneBacRattache,mapMoyenneSansBacRattache,mapMoyenneBacInconnu) }
    udf(redressement)
  }
  def redressementCorrectionInvalide(typeFlux:String,litrage_recipient:String,poids_corr:String,mapMoyenneBacRattache:scala.collection.Map[String,Double],mapMoyenneSansBacRattache:scala.collection.Map[String,Double],mapMoyenneBacInconnu:Double):String={
    (stripAccents(typeFlux),litrage_recipient,poids_corr) match {
      case (_,_,poids_corr) if(poids_corr != null) =>
         BigDecimal(poids_corr).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString.replace(".00",".0")

      case (other,_,_) if(other.matches("Autres|Inconnu")) => // TODO Inconnu peut être a jeter
        log("cas de redressement Autres ou Inconnu, Moyenne bac inconnu utilisé \n valeur: typeFlux -> " + typeFlux + " - litrage_recipient -> " + litrage_recipient + " - poids_corr -> " +  poids_corr)
        mapMoyenneBacInconnu.toString
      //correction des données dechet qui ne sont pas rattaché a des bacs
      case ("OM",null,_) =>
        OptionToValue(mapMoyenneSansBacRattache.get("OMglobal"))
      case ("CS",null,_) =>
        OptionToValue(mapMoyenneSansBacRattache.get("CSglobal"))
      case ("Verre",null,_)  =>
        OptionToValue(mapMoyenneSansBacRattache.get("VEglobal"))
      case ("Biodechets",null,_) =>
        OptionToValue(mapMoyenneSansBacRattache.get("BIOglobal"))
      // correction des données dechets qui sont rattaché a des bacs mais qui n'ont pas au moins 6 valeur historisé
      case ("OM",litrage,_) if(litrage.matches("1|120|140")) =>
        OptionToValue(mapMoyenneBacRattache.get("OM140"))
      case ("OM",litrage,_) if(litrage.matches("180")) =>
        OptionToValue(mapMoyenneBacRattache.get("OM180"))
      case ("OM",litrage,_) if(litrage.matches("240")) =>
        OptionToValue(mapMoyenneBacRattache.get("OM240"))
      case ("OM",litrage,_) if(litrage.matches("330|340|360")) =>
        OptionToValue(mapMoyenneBacRattache.get("OM360"))
      case ("OM",litrage,_) if(litrage.matches("400")) =>
        OptionToValue(mapMoyenneBacRattache.get("OM400"))
      case ("OM",litrage,_) if(litrage.matches("500|660")) =>
        OptionToValue(mapMoyenneBacRattache.get("OM660"))
      case ("OM",litrage,_) if(litrage.matches("750|770|1000")) =>
        OptionToValue(mapMoyenneBacRattache.get("OM770"))
      case ("CS",litrage,_) if(litrage.matches("1|120|140")) =>
        OptionToValue(mapMoyenneBacRattache.get("CS140"))
      case ("CS",litrage,_) if(litrage.matches("180")) =>
        OptionToValue(mapMoyenneBacRattache.get("CS180"))
      case ("CS",litrage,_) if(litrage.matches("240")) =>
        OptionToValue(mapMoyenneBacRattache.get("CS240"))
      case ("CS",litrage,_) if(litrage.matches("330|340|360")) =>
        OptionToValue(mapMoyenneBacRattache.get("CS360"))
      case ("CS",litrage,_) if(litrage.matches("500|660")) =>
        OptionToValue(mapMoyenneBacRattache.get("CS660"))
      case ("CS",litrage,_) if(litrage.matches("750|770|1000")) =>
        OptionToValue(mapMoyenneBacRattache.get("CS770"))
      case ("Verre",litrage,_) if(litrage.matches("240")) =>
        OptionToValue(mapMoyenneBacRattache.get("VE240"))
      case ("Verre",litrage,_) if(litrage.matches("400|660")) =>
        OptionToValue(mapMoyenneBacRattache.get("VE400"))
      case ("Verre",litrage,_) if(litrage.matches("750|770|1000")) =>
        OptionToValue(mapMoyenneBacRattache.get("VE770"))
      case ("Biodechets",litrage,_) if(litrage.matches("120|140")) =>
        OptionToValue(mapMoyenneBacRattache.get("BIO140"))
      case ("Biodechets",litrage,_) if(litrage.matches("240")) =>
        OptionToValue(mapMoyenneBacRattache.get("BIO240"))
      case ("Biodechets",litrage,_) if(litrage.matches("400")) =>
        OptionToValue(mapMoyenneBacRattache.get("BIO400"))
      //correction de données si le litrage récipient n'est pas cohérent
      case ("OM", _, _) =>
        OptionToValue(mapMoyenneSansBacRattache.get("OMglobal"))
      case ("CS", _, _) =>
        OptionToValue(mapMoyenneSansBacRattache.get("CSglobal"))
      case ("Verre", _, _) =>
        OptionToValue(mapMoyenneSansBacRattache.get("VEglobal"))
      case ("Biodechets", _, _) =>
        OptionToValue(mapMoyenneSansBacRattache.get("BIOglobal"))
     //cas par défaut
      case (typeFlux,litrage_recipient,poids_corr) =>
        //log("cas de redressement non cohérent, Moyenne bac inconnu utilisé \n valeur: typeFlux -> " + typeFlux + " - litrage_recipient -> " + litrage_recipient + " - poids_corr -> " +  poids_corr)
        mapMoyenneBacInconnu.toString
    }
  }

  /**
   *
   * @param option_value valeur dont il faut l'extraire la valeur
   * @return  la valeur sans le Some(*)
   */
  def OptionToValue(option_value: Option[Double]): String = {
    option_value match {
      case None => "None"
      case Some(value) => value.toString
    }
  }
  /**
   * permet d'afficher ou non les dataframes dans les différentes étapes selon le niveau de log souhaité
   */
  def show (df:DataFrame,desc:String = null):Unit = {
    val debug = envVar("debugShow")
    debug match {
      case "1" =>
        if (desc != null) {
          log(desc)
        }

      case "2" =>
        if (desc != null) {
          log (desc)
        }
        df.show (20,false)

      case "3" =>
        if (desc != null) {
          log (desc)
        }
        df.show (100000000,false)
      case _ => 
        }

  }

    def getIterator(nameEnv:String, name: String):util.Iterator[_ <: Config] = {
      val listFields = config.getConfigList(nameEnv + "." +name) 
      listFields.iterator()
  }

    def getListName(nameEnv:String): Seq[String]={
    val iterator = Utils.getIterator(nameEnv,"fields")
      var list = List[String]()
      while(iterator.hasNext){
        val current = iterator.next
        list =list :+ current.getString("name")
      }
    list
  }

  /**
   *
   * @param spark   : La session spark
   * @param pgUrl   : l'url de postgresql
   * @param pgTable : le nom de la table a lire dans postres
   */
  def readFomPostgres(spark: SparkSession, pgUrl: String, pgTable: String): DataFrame = {
    if (Utils.envVar("TEST_MODE") == "False") {
      spark.read
        .format("jdbc")
        .option("url", pgUrl)
        .option("dbtable", pgTable)
        .option("user", Utils.envVar("POSTGRES_ACCESS_KEY"))
        .option("password", Utils.envVar("POSTGRES_SECRET_KEY"))
        .load()
    }
    else {
      pgTable match {
        case "dwh.commune_emprise" =>
          val schemaInput = StructType(
            List(
              StructField("objectid", IntegerType, false),
              StructField("code_insee", IntegerType, false),
              StructField("nom", StringType, true),
              StructField("commune_agglo", ShortType, true),
              StructField("x_centrbrg", DoubleType, false),
              StructField("y_centrbrg", DoubleType, true),
              StructField("code_postale", StringType, true),
              StructField("shape", StringType, true)
            )
          )
          spark
            .read
            .option("header", "true")
            .format("csv")
            .schema(schemaInput) // mandatory
            .option("delimiter", ";")
            .load(tableVar("test", "READ_POSTGRES_EMRPISE"))
        case "dwh.quartier" =>
          val schemaInput = StructType(
            List(
              StructField("objectid", IntegerType, false),
              StructField("matricule", StringType, false),
              StructField("nuquart", ShortType, true),
              StructField("nmquart", StringType, true),
              StructField("numnom", StringType, false),
              StructField("nom", StringType, true),
              StructField("shape", StringType, true),
              StructField("code_insee", IntegerType, true),
              StructField("perimetre_geo", DoubleType, true),
              StructField("air_geo", DoubleType, true)
            )
          )
          spark
            .read
            .option("header", "true")
            .format("csv")
            .schema(schemaInput) // mandatory
            .option("delimiter", ";")
            .load(tableVar("test", "READ_POSTGRES_QUARTIER"))
      }

    }
  }
}