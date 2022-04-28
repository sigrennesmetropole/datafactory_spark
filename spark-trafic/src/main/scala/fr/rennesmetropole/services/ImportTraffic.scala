package fr.rennesmetropole.services

import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.functions.{avg, col, count, lit, max, min, stddev, window}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}

// args : WINDOW, DATE,
object ImportTraffic {
  /**
   *
   * @param spark : la session spark
   * @param DATE  : la date de la semaine a analysé
   * @return
   */
  def ExecuteImportTraffic(spark : SparkSession, DATE: String): DataFrame ={
    val readedDf = readFromMinio(spark,DATE)
    formatTraffic(spark, readedDf)
  }


  /**
   *
   * @param spark : la session spark
   * @param DATE  : date de la semaine a analysé
   * @return
   */
  def readFromMinio(spark : SparkSession, DATE: String): DataFrame = {


    /** Structure de la donnée dans Minio */
    val schema = StructType(
      List(
        StructField("datetime", StringType, true),
        StructField("predefinedLocationReference", StringType, false),
        StructField("averageVehicleSpeed", IntegerType, false),
        StructField("travelTime", IntegerType, false),
        StructField("travelTimeReliability", IntegerType, false),
        StructField("trafficStatus", StringType, false)
      )
    )

    /** Lecture de la donnée dans Minio */
    Utils.readData(spark, schema, DATE)

  }

  /**
   *
   * @param spark : la session spark
   * @param df    : le dataframe à formater
   * @return
   */
  def formatTraffic(spark : SparkSession, df : DataFrame): DataFrame = {


    /** Preparation des nouveaux noms de colonnes */
    val newColumnsNames = Map("dateTime" -> "date",
      "predefinedLocationReference" -> "segment",
      "averageVehicleSpeed" -> "avgSpeed",
      "travelTime" -> "travelTime",
      "travelTimeReliability" -> "reliability",
      "trafficStatus" -> "status")

    /** renommage des colonnes de la donnée */
    val dfFormatted = Utils.formatData(df, newColumnsNames)

    /** formatage de la date */
    val dfWithDateFormated = Utils.formatColumnDate(dfFormatted, "date", "yyyy-MM-dd'T'HH:mm:ss")

    /** On applique l'analyse uniquement sur les mesures prises entre 5h et 21h */
    val dfFilteredOnHours = Utils.filterDataframeBetweenTwoHours(dfWithDateFormated, Utils.envVar("START_HOUR_FILTER").toInt, Utils.envVar("END_HOUR_FILTER").toInt)
    dfFilteredOnHours


  }
}
