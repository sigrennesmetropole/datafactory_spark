package fr.rennesmetropole.tools

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.services.DechetAnalysis.typeFrequence
import fr.rennesmetropole.services.ImportDechet.{createEmptyBacDataFrame, createEmptyCollecteDataFrame, createEmptyExutoireDataFrame, createEmptyProducteurDataFrame}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import java.sql.{Connection, DriverManager, SQLException, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util.Properties
import java.math.BigInteger 

object Utils {

  val logger = Logger(getClass.getName)
  var config = ConfigFactory.load()

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
    val URL = tableVar(nameEnv, "out_bucket")
    
    if(Utils.envVar("TEST_MODE") == "False") {

      /* calcul du chemin pour lire les données sur minio */
      val postURL = date2URL(DATE)
      println("URL de lecture sur Minio : " + URL + postURL)

         try {
        spark
            .read
            .orc(URL+postURL)
          
        } catch {
          case e : Throwable =>
          println("ERROR while reading data at : " + URL+postURL + " \nError stacktrace :"+ e)
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
        }


    // else... si nous somme en mode TEST
    }else {
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
          spark
            .read
            .option("header", "true")
            .option("compression", "gzip")
            .format("csv")
            .option("delimiter", ";")
            .load(URL)

        }
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
      val year = date(0);
      val month = date(1);
      val day = date(2);
      val postURL = "year=" + year + "/month=" + month + "/day=" + day + "/";

      return postURL
    }

  /**
   *
   * @param spark     : La session spark
   * @param pgUrl     : l'url de postgresql
   * @param dfToWrite : le dataframe à écrire dans postgres
   * @param pgTable   : le nom de la table dans postres
   */
  def postgresPersist(spark: SparkSession, pgUrl: String, dfToWrite: DataFrame, pgTable: String,DATE : String): Unit = {

    println("Proceed to write data in table " + pgTable)

    val connectionProps = new Properties()
    connectionProps.setProperty("driver", "org.postgresql.Driver")
    connectionProps.setProperty("user", Utils.envVar("POSTGRES_ACCESS_KEY"))
    connectionProps.setProperty("password", Utils.envVar("POSTGRES_SECRET_KEY"))

    val nb = delete_partition(pgTable,pgUrl,DATE)
    println("fonction suppression fini ")

    //Passing in the URL, table in which data will be written and relevant connection properties
    dfToWrite.write.mode(SaveMode.Append).jdbc(pgUrl, pgTable, connectionProps)
  }

   def postgresPersistOverwrite(spark: SparkSession, pgUrl: String, dfToWrite: DataFrame, pgTable: String): Unit = {

    println("Proceed to replace all data in table " + pgTable)

    val connectionProps = new Properties()
    connectionProps.setProperty("driver", "org.postgresql.Driver")
    connectionProps.setProperty("user", Utils.envVar("POSTGRES_ACCESS_KEY"))
    connectionProps.setProperty("password", Utils.envVar("POSTGRES_SECRET_KEY"))

    val nb = delete_table(pgTable,pgUrl)
    println("fonction suppression fini ")

    //Passing in the URL, table in which data will be written and relevant connection properties
    dfToWrite.write.mode(SaveMode.Append).jdbc(pgUrl, pgTable, connectionProps)
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
          println(number_of_rows_deleted + " rows deleted.")
      }
    }
    catch {
      case e:SQLException => println("error lors le la suppression des valeurs partitionnées"); e.printStackTrace();
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
      var stringDelete = "TRUNCATE TABLE "+ table_name
      val statement = connObj.prepareStatement(stringDelete)
      try{
          number_of_rows_deleted = statement.executeUpdate();
      }
      finally{
          statement.close();
          println(number_of_rows_deleted + " rows deleted.")
      }
    }
    catch {
      case e:SQLException => println("error lors le la suppression de la table " + table_name + " : "); e.printStackTrace();
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
    println("Write to s3 to " + Utils.tableVar(nameEnv,"analysed_bucket") + postURL)

    df_toWrite.repartition(1)   // nécessaire pour écrire le DF dans un seul csv, mais pPeut poser problème si le DF est trop gros
      .write.options(Map("header"->"true", "delimiter"->";","compression"->"gzip"))
      .mode(SaveMode.Append)
      .csv(Utils.tableVar(nameEnv,"analysed_bucket") + postURL)

    if("tableProducteur".equals(nameEnv) || "tableRecipient".equals(nameEnv) ) {

      df_toWrite.repartition(1)   // nécessaire pour écrire le DF dans un seul csv, mais pPeut poser problème si le DF est trop gros
        .write
        .mode(SaveMode.Append)
        .orc(Utils.tableVar(nameEnv,"analysed_bucket") + postURL+"orc/")

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

    println("Write to s3 Done")
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

}