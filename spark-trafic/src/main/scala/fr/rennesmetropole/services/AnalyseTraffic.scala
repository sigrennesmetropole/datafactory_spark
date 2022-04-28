package fr.rennesmetropole.services

import org.apache.spark.sql.functions.{avg, bround, col, count, max, min, round, stddev, window}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AnalyseTraffic {
  /**
   *
   * @param spark  : la session spark
   * @param df     : le dataframe à traiter
   * @param WINDOW : la fenêtre en minutes, par défaut à 15
   * @return
   */
  def ExecuteAnalyseTraffic(spark : SparkSession, df : DataFrame, WINDOW : Int): DataFrame ={
    /** Traitement principal de la donnée */
    val dfProcessed = df.groupBy(col("segment"), window(col("date"), WINDOW + " minutes"))
      .agg(min("avgSpeed").as("speed_min"),
        round(avg("avgSpeed"),1).as("speed_avg"),
        max("avgSpeed").as("speed_max"),
        round(stddev("avgSpeed"),1).as("speed_std"),
        round(avg("reliability"),1).as("rel_avg"),
        round(stddev("reliability"),1).as("rel_std"),
        count("avgSpeed").as("nb_values"))

    /** Sélection de la donnée à écrire dans Postgres */
    dfProcessed.select(
      "window.start",
      "segment",
      "speed_min",
      "speed_avg",
      "speed_max",
      "speed_std",
      "rel_avg",
      "rel_std",
      "nb_values"
    ).withColumnRenamed("start", "start_time")
      .withColumnRenamed("segment", "id")
  }
}
