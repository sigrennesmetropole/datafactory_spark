package fr.rennesmetropole.services

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object DechetExutoirePreparation {
  val logger = Logger(getClass.getName)


  /**
   * Fonction qui permet de créer un dataframe avec les données référentiels
   *
   * @param spark : la session spark
   * @param df    : le dataframe à traiter
   * @return
   */

  def ExecuteDechetExutoirePreparation(spark: SparkSession, df_raw_exutoire: DataFrame, nameEnv:String): DataFrame = {
     //df_imported = df_raw_exutoire
    val(mapNbtoName,mapNametoParam) = Utils.getMap(spark,nameEnv)
/*
    println("--------------df BEFORE renommageColonnes------------")
    df_imported.show(5)

    if(Utils.tableVar(nameEnv,"header") == "True"){
      //Renommage des colonnes
      df_imported = Utils.renommageColonnes(spark,df_raw_exutoire,mapNbtoName)
      println("--------------df AFTER renommageColonnes------------")
      df_imported.show(5)
    } */
    
    /* println("--------------df AFTER regexCharSpe------------") */
    val df_withoutAccentAndSpace = Utils.regexCharSpe(spark, df_raw_exutoire)
      .select("immat","date_service_vehic","description_tournee","km_realise","no_bon","lot","service","nom_rech_lieu_de_vidage","multiples_lignes","cle_unique_ligne_ticket")
    /* df_withoutAccentAndSpace.show(5) */

    println("--------------df AFTER lowerCaseAllHeader df_lowered------------")
    var df_lowered = Utils.lowerCaseAllHeader(spark, df_withoutAccentAndSpace)
    df_lowered = Utils.castDF(spark,df_lowered,nameEnv,mapNametoParam)
    println("--------------df_lowered après le cast------------")
    df_lowered.printSchema()
    df_lowered.show(5)

    val df_typed = df_lowered
      .withColumn("immat",new Column(AssertNotNull(col("immat").expr)))
      .withColumn("km_realise",new Column(AssertNotNull(col("km_realise").expr)))  //ex IntegerType
      .withColumn("service",new Column(AssertNotNull(col("service").expr))) //ex IntegerType

    println("--------------df_typed------------")
    df_typed.printSchema()
    df_typed.show(5)
    df_typed
  }
}