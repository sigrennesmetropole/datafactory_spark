package fr.rennesmetropole.services

import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.rennesmetropole.tools.Utils.log

object ImportDechetExutoire {


  /**
   * Declare un dataframe via la lecture par date dans MinIO 
   *
   * @param spark : la session spark
   * @param DATE  : la date de la semaine à analyser
   * @return
   */
  def ExecuteImportDechetExutoire (spark: SparkSession, DATE: String, nameEnv: String): DataFrame = {
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
      Utils.readData(spark, DATE, nameEnv)
    } catch {
      case e: Throwable => {
        logger.error("Erreur de chargement des fichiers depuis MinIO") 
        throw new Exception("Erreur de chargement des fichiers depuis MinIO", e)
      } 
    } 
  }
}