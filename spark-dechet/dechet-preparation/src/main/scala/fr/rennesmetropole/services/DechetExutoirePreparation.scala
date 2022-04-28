package fr.rennesmetropole.services

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.tools.{UDF, Utils}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, FloatType}
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
    /*var df_imported = df_raw_exutoire
    val(mapNbtoName,mapNametoParam) = Utils.getMap(spark,nameEnv)

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
    /* df_withoutAccentAndSpace.show(5) */

    /* println("--------------df AFTER lowerCaseAllHeader------------") */
    val df_lowered = Utils.lowerCaseAllHeader(spark, df_withoutAccentAndSpace)
    //df_lowered.show(5)
    val df_typed = df_lowered.withColumn("immat",new Column(AssertNotNull(col("immat").cast(StringType).expr)))   
      .withColumn("date_service_vehic",col("date_service_vehic").cast(StringType)) 
      .withColumn("description_tournee",col("description_tournee").cast(StringType))  //ex IntegerType
      .withColumn("km_realise",new Column(AssertNotNull(col("km_realise").cast(DoubleType).expr)))  //ex IntegerType
      .withColumn("no_bon",col("no_bon").cast(StringType))
      .withColumn("lot",col("lot").cast(StringType)) 
      .withColumn("service",new Column(AssertNotNull(col("service").cast(DoubleType).expr))) //ex IntegerType
      .withColumn("nom_rech_lieu_de_vidage",col("nom_rech_lieu_de_vidage").cast(StringType))
      .withColumn("multiples_lignes",col("multiples_lignes").cast(DoubleType))
      .withColumn("cle_unique_ligne_ticket", col("cle_unique_ligne_ticket").cast(StringType))  //ex FloatType

    /* df_typed.printSchema()

    println("--------------df_typed------------") */
    df_typed.show(5) 
    df_typed
  }
}