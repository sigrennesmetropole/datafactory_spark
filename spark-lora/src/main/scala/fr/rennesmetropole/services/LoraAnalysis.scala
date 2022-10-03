package fr.rennesmetropole.services

import fr.rennesmetropole.tools.{Traitements, Utils}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.json4s._
import fr.rennesmetropole.tools.Utils.{log, show}

object LoraAnalysis {

  /**
   * Fonction qui permet de créer un dataframe avec les donnée mise à plat et de merger le résultat des traitements 
   * de chaque capteurs sur ce dataframe
   *
   * @param spark : la session spark
   * @param df    : le dataframe à traiter
   * @return
   */
  def ExecuteLoraAnalysis(spark: SparkSession, df_raw: DataFrame, deveui: String, df_step: DataFrame): DataFrame = {
    /** Schéma representant la sortie des données reçu mise à plat */
    val schema = StructType(
      List(
        StructField("id", StringType, false),
        StructField("deveui", StringType, false),
        StructField("timestamp", StringType, false),
        StructField("name", StringType, false),
        StructField("value", FloatType),
        StructField("inserteddate", FloatType),
        StructField("year", StringType, false),
        StructField("month", StringType, false),
        StructField("day", StringType, false)
      )
    )

    /** Nécéssaire pour la conversion en dictionnaire */
    implicit val formats = DefaultFormats

    /** Map du dictionnaire de deveui capteurs / noms traitements sur les fonctions de traitement */
    val df_param = Utils.readFomPostgres(spark,Utils.envVar("POSTGRES_URL"), Utils.envVar("POSTGRES_TABLE_PARAM_NAME"))
    val param = Utils.getParam(df_param)
    Traitements.traitement_pre_traitement(df_raw.filter(df_raw("deveui") === deveui), deveui, spark, param(deveui), df_step)

  }

  /**
   * Fonction qui permet d'ajouter au dataframe les information nécessaire situé dans une table référentiel de postgres
   *
   * @param spark : spark session a transmettre
   * @param df    : dataframe sur lequel on va joindre les données
   */
  def enrichissement_Basique(spark: SparkSession, df: DataFrame): DataFrame = {
    val schema = StructType(
      List(
        StructField("channel", StringType, false),
        StructField("deveui", StringType, false),
        StructField("enabledon", StringType, false),
        StructField("disabledon", StringType, false),
        StructField("measurenatureid", StringType, false),
        StructField("unitid", StringType, false),
        StructField("dataparameter", StringType, false),
        StructField("year", StringType, false),
        StructField("month", StringType, false),
        StructField("day", StringType, false)
      )
    )

    /** Lecture des données depuis une table postgres puis on ne selectionne que les données qui vont être joint a notre dataframe */
    var df_measureInfo = if (Utils.envVar("TEST_MODE") == "False") {
      Utils.readFomPostgres(spark, Utils.envVar("POSTGRES_URL"), "lora_referential.v_measureinformation").select("deveui", "enabledon", "disabledon", "measurenatureid", "unitid", "dataparameter").withColumnRenamed("dataparameter", "name")
    } else {
      spark
        .read
        .option("header", "true")
        .format("csv") // "minioSelectJSON" for JSON or "minioSelectParquet" for Parquet
        .schema(schema) // mandatory
        .option("delimiter", ",")
        .load("src/test/resources/Local/services/v_mesureinformationInput.csv").withColumnRenamed("dataparameter", "name")
    }

    /** Condition sur la jointure pour selectionner les bonnes références */
    val condition_jointure = df("tramedate") >= df_measureInfo("enabledon") && (df("tramedate") < df_measureInfo("disabledon") || df_measureInfo("disabledon").isNull)

    val df_join = df.alias("a").join(df_measureInfo.alias("b"), df("deveui") === df_measureInfo("deveui") && df("name") === df_measureInfo("name") && condition_jointure, "inner")

    /** On selectionne les bonnes colonnes pour ne pas avoir de colonnes en double suite a la jointure */

    val df_join_clean = df_join.select("a.id", "a.deveui", "b.enabledon", "a.tramedate", "a.name", "a.value", "a.insertedDate", "b.measurenatureid", "b.unitid","a.year","a.month","a.day")
      .withColumnRenamed("insertedDate","inserteddate")

    df_join_clean
  }
}