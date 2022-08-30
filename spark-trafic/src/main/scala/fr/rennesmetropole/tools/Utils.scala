package fr.rennesmetropole.tools

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{concat_ws, lit}
import java.util.Properties
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException


import java.text.SimpleDateFormat
import java.util.{Calendar, Date}


object Utils {

  var config = ConfigFactory.load()
  var URL = Utils.envVar("READ_URL")
  var FORMAT = Utils.envVar("READ_FORMAT")

  /**
   *
   * @param name Nom de la configuration
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
   * Declare les propriétés pour se connecter à la base de donnée PostgreSQL
   * @return : un objet Properties permettant de se connecter à PostGres
   */
  def create_pg(): Properties = {
    val connectionProps = new Properties()
    connectionProps.setProperty("driver", "org.postgresql.Driver")
    connectionProps.setProperty("user", sys.env("PG_USERNAME"))
    connectionProps.setProperty("password", sys.env("PG_PASSWORD"))
    connectionProps
  }

  /**
   * @param spark  : la session spark
   * @param url    : url complète du stockage
   * @param format : le format des données, CSV, minioSelectCSV
   * @param schema : le schéma des données à lire
   * @return Dataframe
   */
  def readData(spark: SparkSession, schema: StructType, DATE: String): DataFrame = { 
  val postURL = date2URL(DATE)
   if(Utils.envVar("TEST_MODE") == "False") {
     try {
      spark
          .read
          .option("compression", "gzip")
          .format(FORMAT) // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
          .schema(schema) // mandatory
          .option("delimiter", ";")
          .load(URL+postURL)
     } catch {
        case e : Throwable =>
        println("ERROR URL : " + URL+postURL + "  does not exist, your date may be not correct " + e)
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
      }

    }else {
      if(postURL.split("/")(2).split("=")(1)!="-1"){
        spark
          .read
          .option("compression", "gzip")
          .format(FORMAT) // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
          .schema(schema) // mandatory
          .option("delimiter", ";")
          .load(URL)
      }else {
        println("ERROR DATE : " + DATE + " your date may be not correct ")
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
      }

    }

  }
/**
 * Permet de prendre une date en input et de ressortir la version partitionne
 * @param DATE date sous forme  yyyy-mm-dd
 * @return string sous la forme yyyy/mm/ss ou ss est le numero de la semaine du mois courant
 */
def date2URL(DATE: String): String = {
  val date = DATE.toString.split("-")  // donne la date sous forme yyyy-mm-dd
  val year = date(0);
  val month = date(1);
  val day = date(2);
  val week = day2Week(day.toInt)
  val postURL = "year="+ year + "/month="+ month +"/week=" + week+"/";

  return postURL
}

/**
 * Permet de savoir selon une date donne, dans quelle semaine du mois on se trouve
 * @param day jour du mois
 * @return retourne le numéro de semaine (du mois donc entre 1 et 4)
 */
def day2Week(day : Int ):String = {
  var nbWeek="-1";
   (Math.floor((day-1)/7)) match {  // permet de savoir dans quelle semaine on est | -1 pour que le 7e jour soit compte comme semaine 1
    case 0 =>
      nbWeek = "01"
    case 1 =>
      nbWeek = "02"
    case 2=>
      nbWeek = "03"
    case 3=>
      nbWeek = "04"
    case 4=>
      nbWeek = "05"
    case _=>
      nbWeek="-1"

  }
  return nbWeek;
}

  /**
   *
   * @param df         : Dataframe à formater
   * @param columnsMap : Map des colonnes à renommer
   * @return Dataframe formaté
   */
  def formatData(df: DataFrame, columnsMap: Map[String, String]): DataFrame = {
    val dfWithRenamedColumns = columnsMap.foldLeft(df) { (acc, names) =>
      acc.withColumnRenamed(names._1, names._2)
    }
    dfWithRenamedColumns
  }

  /**
   * Donne les accès pour se connecter à Postgresql et procède à l'écriture en mode APPEND
   * @param spark     : La session spark
   * @param pgUrl     : l'url de postgresql
   * @param dfToWrite : le dataframe à écrire dans postgres
   * @param pgTable   : le nom de la table dans postres
   * @param DATE      : la Date a laquelle le job est e
   */
  def postgresPersist(spark: SparkSession, pgUrl: String, dfToWrite: DataFrame, pgTable: String, DATE : String): Unit = {
    val connectionProps = new Properties()
    connectionProps.setProperty("driver", "org.postgresql.Driver")
    connectionProps.setProperty("user", Utils.envVar("POSTGRES_ACCESS_KEY"))
    connectionProps.setProperty("password", Utils.envVar("POSTGRES_SECRET_KEY"))


    val nb = delete_partition(pgTable,pgUrl,DATE)
    println("DEBUG DELETE suppression fini ")

    /** Transmission de l'URL, du tableau dans lequel les données seront écrites et des propriétés de connexion pertinentes */
    println("Proceed to write data in table " + pgTable)
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
    val week = day2Week(arrayDate(2).toInt)
    val driverClass = "org.postgresql.Driver"
    var connObj:Connection = null
    var number_of_rows_deleted:Int = 0
    try{
      Class.forName(driverClass);
      connObj = DriverManager.getConnection(pgUrl,  Utils.envVar("POSTGRES_ACCESS_KEY"), Utils.envVar("POSTGRES_SECRET_KEY"));
      val statement = connObj.prepareStatement("DELETE FROM "+ table_name +" WHERE  year ='"+ arrayDate(0) +"' AND month='"+ arrayDate(1) +"' AND  week='"+ week+"'")
      try{
          number_of_rows_deleted = statement.executeUpdate();
      }
      finally{
          statement.close();
          println(number_of_rows_deleted + " rows deleted.")
      }
    }
    catch {
      case e:SQLException => e.printStackTrace();
    }
    finally{
      connObj.close();
      println("connection closed [delete]")
    }
  
  }

  /**
   * Converti la colonne date du dataframe en une valeur du type de données TIMESTAMP.
   * @param df         : le dataframe à parser
   * @param columnName : le nom de la colonne du dataframe à parser
   * @param pattern    : le pattern de la date
   * @return le dataframe avec la date parsée
   */
  def formatColumnDate(df: DataFrame, columnName: String, pattern: String): DataFrame = {
    val dfWithDateFormated = df.withColumn("date", to_timestamp(substring(col(columnName),1,19), pattern))
    dfWithDateFormated
  }

  /**
   * Permet de mettre au bon format Date (en entrée et en  sortie)
   * @param date : La date au format String à parser
   * @return la date au format ISO parsée
   */
  def parseDateISO(date: String): String = {
    val formatInput = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    val formatOutput = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    formatOutput.format(formatInput.parse(date))
  }

  /**
   * Permet de filtrer le dataframe entre 2 heures selectionnées
   * @param df le dataframe a filtrer
   * @param hourFrom l'heure de début du filtre = 4
   * @param hourTo l'heure de fin du filtre = 22
   * @return
   */
  def filterDataframeBetweenTwoHours(df : DataFrame, hourFrom : Int, hourTo : Int): DataFrame ={
    df.select("*").where(hour(col("date")).gt(hourFrom) and(hour(col("date")).lt(hourTo)))
  }

  /**
   * Arrondi la date au dernier quart d'heure
   * @param date la date à arrondir au dernier quart d'heure
   * @return
   */
  def roundDateToLatestQuarter(date : Date) : Date ={
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    val unroundedMinutes = calendar.get(Calendar.MINUTE)
    val mod = unroundedMinutes % Utils.envVar("WINDOW").toInt
    calendar.add(Calendar.MINUTE, -mod)
    calendar.set(Calendar.SECOND,0)
    println("calendar")
    println(calendar.toString)
    calendar.getTime
  }

  /**
   * Supprime les données du dataframe via la date correspondante en entrée
   * @param df le dataframe à traiter
   * @param date la date sur laquelle l'on va filter les données
   * @return
   */
  def removeDataFromDate(df : DataFrame, date : Date) : DataFrame = {
    df.select("*").where(col("date").lt(new java.sql.Date(date.getTime())))
  }

  /**
  * Manipule les valeurs des colonnes du dataframe :
  * Recupération de Year, Month et Week + Création de la colonne technical-key (par concaténation de id, start_time, year, month et week)
  * @param df le dataframe à traiter
  * @return le daframe en entré plus une colonne qui va servir de partitionnement
  */
  def dfToPartitionedDf(df : DataFrame, DATE :String) : DataFrame = {
    val arrayDate = DATE.split("-")
    val week = day2Week(arrayDate(2).toInt)
    val df2 = df.withColumn("year",lit(arrayDate(0))).withColumn("month",lit(arrayDate(1))).withColumn("week",lit(week))
    df2.withColumn("technical_key", concat_ws("_", col("id"), col("start_time"), col("year"), col("month"), col("week")))
  
  }
}
