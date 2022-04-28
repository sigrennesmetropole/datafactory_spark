package fr.rennesmetropole.services

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.tools.{Utils}
import org.apache.spark.sql._


object ImportDechet {

  val logger = Logger(getClass.getName)

  /**
   * Declare un dataframe via la lecture par date dans MinIO 
   *
   * @param spark : la session spark
   * @param DATE  : la date de la semaine à analyser
   * @return
   */
  def ExecuteImportDechet(spark: SparkSession, DATE: String, nameEnv:String): DataFrame = {
    val schema = Utils.getSchema(nameEnv)
    Utils.readData(spark, DATE, schema,nameEnv)
  }
}