package fr.rennesmetropole.services

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


object ImportLora {

  val logger = Logger(getClass.getName)

  /**
   * Declare un dataframe via la lecture par date dans MinIO 
   *
   * @param spark : la session spark
   * @param DATE  : la date de la semaine à analyser
   * @return
   */
  def ExecuteImportLora(spark: SparkSession, DATE: String, deveui: String): DataFrame = {
    readFromMinio(spark, DATE, deveui)
  }

  /**
   *
   * @param spark : la session spark
   * @param DATE  : date de la semaine à analyser
   * @return
   */
  def readFromMinio(spark: SparkSession, DATE: String, deveui: String): DataFrame = {
    /** Définition du schema du dataframe */
    val schema = StructType(
      List(
        StructField("deveui", StringType),
        StructField("sensor_id", StringType),
        StructField("timestamp", StringType),
        StructField("data", StringType)
      )
    )

    /** Lecture de la donnée dans Minio */
    try {
      Utils.readData(spark, DATE, schema, deveui)
    } catch {
      case e: Throwable => {
        logger.error("Erreur de chargement des fichiers depuis MinIO") 
        throw new Exception("Erreur de chargement des fichiers depuis MinIO", e)
      }
    }
  }
}