package fr.rennesmetropole.services

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.tools.{Utils}
import org.apache.spark.sql.{DataFrame, SparkSession}


object ImportDechetRef {

  val logger = Logger(getClass.getName)

  /**
   * Declare un dataframe via la lecture par date dans MinIO 
   *
   * @param spark : la session spark
   * @param DATE  : la date de la semaine à analyser
   * @return
   */
  def ExecuteImportDechetRef(spark: SparkSession, DATE: String, nameEnv: String): DataFrame = {
    readFromMinio(spark, DATE, nameEnv)
  }

  /**
   *
   * @param spark : la session spark
   * @param DATE  : date de la semaine à analyser
   * @return
   */
  def readFromMinio(spark: SparkSession, DATE: String, nameEnv: String): DataFrame = {
  
    /** Lecture de la donnée dans Minio */
    try {
      val schema = Utils.getSchema(nameEnv)
      Utils.readData(spark, DATE, schema, nameEnv)
    } catch {
      case e: Throwable => {
        println("Erreur de chargement des fichiers depuis MinIO - stacktrace : \n" + e) //necessaire de print la stack trace pour pouvoir ensuite analyser les logs
        throw new Exception("Erreur de chargement des fichiers depuis MinIO => ", e)
      } 
    } 
  }
}