package fr.rennesmetropole.tools

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.spark.sql.expressions.UserDefinedFunction

object Utils {

  val logger = LogManager.getLogger(getClass.getName)
  val USER_LOG = Level.forName("DATA FACTORY", 200);
  var config = ConfigFactory.load()
  var URL = Utils.envVar("READ_URL")

  def log(msg: Any): Unit = {
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
   * @param spark  : la session spark
   * @param url    : url complète du stockage
   * @param format : le format des données, JSON, minioSelectJSON
   * @param schema : le schéma des données à lire
   * @return Dataframe
   */
  def readData(spark: SparkSession, DATE: String, schema: StructType, deveui: String): DataFrame = {
    // get all file paths
    val path = if (Utils.envVar("TEST_MODE") == "True") {
      URL
    } else {
      var postURL = date2URL(DATE)
      println("Reading data from : " + URL + postURL + deveui +"/")
      URL + postURL + deveui +"/"
    }
    try {
      spark.read.option("multiline", "true").json(path)
    } catch {
      case e: Throwable => {
        println("ERROR URL : " + path + "  does not exist, your date may be not correct " + e)
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
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
  def postgresPersist(spark: SparkSession, pgUrl: String, dfToWrite: DataFrame, pgTable: String,DATE : String,deveui: String): Unit = {

    log("Proceed to write data in table " + pgTable)

    val connectionProps = new Properties()
    connectionProps.setProperty("driver", "org.postgresql.Driver")
    connectionProps.setProperty("user", Utils.envVar("POSTGRES_ACCESS_KEY"))
    connectionProps.setProperty("password", Utils.envVar("POSTGRES_SECRET_KEY"))

    val nb = delete_partition(pgTable,pgUrl,DATE, deveui)
    log("fonction suppression fini ")

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
  def delete_partition(table_name:String, pgUrl: String, DATE:String, deveui:String){
   log("delete_partition")
    val arrayDate = DATE.split("-")
    val driverClass = "org.postgresql.Driver"
    var connObj:Connection = null
    var number_of_rows_deleted:Int = 0
    try{
      Class.forName(driverClass);
      connObj = DriverManager.getConnection(pgUrl,  Utils.envVar("POSTGRES_ACCESS_KEY"), Utils.envVar("POSTGRES_SECRET_KEY"));
      var sqlStr = "DELETE FROM "+ table_name +" WHERE year='"+ arrayDate(0) +"' AND month='"+ arrayDate(1) +"' AND  day='"+ arrayDate(2)+"'"
      if(StringUtils.isNotBlank(deveui)){
        sqlStr += " AND deveui='"+deveui+"'"
      }
      val statement = connObj.prepareStatement(sqlStr)
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
    }
  
  }

  def postgresPersist_Reprise(spark: SparkSession, pgUrl: String, dfToWrite: DataFrame, pgTable: String, DATE_debut: String,DATE_fin: String, deveui: String): Unit = {

    log("Proceed to write data in table " + pgTable)

    val connectionProps = new Properties()
    connectionProps.setProperty("driver", "org.postgresql.Driver")
    connectionProps.setProperty("user", Utils.envVar("POSTGRES_ACCESS_KEY"))
    connectionProps.setProperty("password", Utils.envVar("POSTGRES_SECRET_KEY"))

    val nb = delete_partition_reprise(pgTable, pgUrl, DATE_debut, DATE_fin, deveui)
    log("fonction suppression fini")

    //Passing in the URL, table in which data will be written and relevant connection properties
    dfToWrite.write.mode(SaveMode.Append).jdbc(pgUrl, pgTable, connectionProps)
  }

  def delete_partition_reprise(table_name: String, pgUrl: String, DATE_debut: String,DATE_fin: String, deveui: String) {
    log("delete_partition_reprise")
    val arrayDate_debut = DATE_debut.split("-")
    val arrayDate_fin = DATE_fin.split("-")
    val driverClass = "org.postgresql.Driver"
    var connObj: Connection = null
    var number_of_rows_deleted: Int = 0
    try {
      Class.forName(driverClass);
      connObj = DriverManager.getConnection(pgUrl, Utils.envVar("POSTGRES_ACCESS_KEY"), Utils.envVar("POSTGRES_SECRET_KEY"));
      var sqlStr = "DELETE FROM " + table_name + " WHERE concat(year, '-',month,'-',day)>='" + arrayDate_debut(0)+"-"+arrayDate_debut(1)+"-"+arrayDate_debut(2) +"' AND concat(year, '-',month,'-',day)<='" + arrayDate_fin(0)+"-"+arrayDate_fin(1)+"-"+arrayDate_fin(2) +"'"
      if (StringUtils.isNotBlank(deveui)) {
        sqlStr += " AND deveui='" + deveui + "'"
      }
      val statement = connObj.prepareStatement(sqlStr)
      try {
        number_of_rows_deleted = statement.executeUpdate();
      }
      finally {
        statement.close();
        println(number_of_rows_deleted + " rows deleted.")
      }
    }
    catch {
      case e: SQLException => e.printStackTrace();
    }
    finally {
      connObj.close();
    }

  }
  def updateSensor_Reprise(table_name: String, pgUrl: String, DATE: String, deveui: String) {
    log("updateSensor_Reprise pour le capteur : " + deveui)
    val driverClass = "org.postgresql.Driver"
    var connObj: Connection = null
    var number_of_rows_deleted: Int = 0
    try {
      Class.forName(driverClass);
      connObj = DriverManager.getConnection(pgUrl, Utils.envVar("POSTGRES_ACCESS_KEY"), Utils.envVar("POSTGRES_SECRET_KEY"));
      val sqlReq =s"""
           |UPDATE $table_name
           |SET "reprisedon" = '$DATE'
           |WHERE deveui = '$deveui'
           |""".stripMargin

      val statement = connObj.prepareStatement(sqlReq)
      try {
        number_of_rows_deleted = statement.executeUpdate();
      }
      finally {
        statement.close();
        println(number_of_rows_deleted + " rows updated.")
      }
    }
    catch {
      case e: SQLException => e.printStackTrace();
    }
    finally {
      connObj.close();
    }
log("Update termine")
  }


  /**
   *
   * @param spark   : La session spark
   * @param pgUrl   : l'url de postgresql
   * @param pgTable : le nom de la table a lire dans postres
   */
  def readFomPostgres(spark: SparkSession, pgUrl: String, pgTable: String): DataFrame = {
    if(Utils.envVar("TEST_MODE") == "False") {
      spark.read
        .format("jdbc")
        .option("url", pgUrl)
        .option("dbtable", pgTable)
        .option("user", Utils.envVar("POSTGRES_ACCESS_KEY"))
        .option("password", Utils.envVar("POSTGRES_SECRET_KEY"))
        .load()
    }
    else{
      spark
        .read
        .option("header", "true")
        .format("csv")
        .option("delimiter", ";")
        .load(envVar("READ_SENSOR_TEST"))
    }
  }

  def getParam(df: DataFrame): Map[String,Array[Seq[String]]] = {
    var map = Map[String,Array[Seq[String]]]()
    println("GET PARAM")
    df.show(10,false)
    for (r <- df.rdd.collect) {   
      // une row r se peu se presenter comme ceci : 
      //  exemple : 70b3d5e75e003dcd-WrappedArray(PTCOUR1)-WrappedArray(PTCOUR1)-WrappedArray(EAP_s, EAP_i)-WrappedArray()-WrappedArray()-WrappedArray(EAP_s, EAP_i)-WrappedArray()
      //Il faut donc enlever tout les WrappedArray(...) et les espaces en trop mis après les ',' 
      var row = r.mkString("-").replace("WrappedArray","").replace(", ",",").replace("(","").replace(")","").split("-",-1) // -1 pour eviter que le split supprime les valeurs empty en fin de tableau
      
      val seq1 = row(1).split(",").toSeq.filter(_.nonEmpty)
      val seq2 = row(2).split(",").toSeq.filter(_.nonEmpty)
      val seq3 = row(3).split(",").toSeq.filter(_.nonEmpty)
      val seq4 = row(4).split(",").toSeq.filter(_.nonEmpty)
      val seq5 = row(5).split(",").toSeq.filter(_.nonEmpty)
      val seq6 = row(6).split(",").toSeq.filter(_.nonEmpty)
      val seq7 = row(7).split(",").toSeq.filter(_.nonEmpty)
      val seq8 = (seq1++:seq2++:seq3++:seq4++:seq5++:seq6).distinct.filter(_.nonEmpty)
      map += (row(0) -> Array(seq1,seq2,seq3,seq4,seq5,seq6,seq7,seq8))
    }
    map
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
    df2.withColumn("technical_key", concat_ws("_", col("id"), col("year"), col("month"), col("day")))
  
  }
  def dfToPrePartitionedDf_Reprise(df : DataFrame, DATE :String) : DataFrame = {
    val arrayDate = DATE.split("-")
    if(df.columns.contains("year")){
      df.withColumn("year",when(df("year").isNull,lit(arrayDate(0))).otherwise(df("year"))).withColumn("month",when(df("month").isNull,lit(arrayDate(1))).otherwise(df("month"))).withColumn("day",when(df("day").isNull,lit(arrayDate(2))).otherwise(df("day")))
    }else {
      df.withColumn("year",lit(arrayDate(0))).withColumn("month",lit(arrayDate(1))).withColumn("day",lit(arrayDate(2)))
    }

  }

  def dfToPostPartitionedDf_Reprise(df: DataFrame): DataFrame = {
    df.withColumn("technical_key", concat_ws("_", col("id"), col("year"), col("month"), col("day")))

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
}