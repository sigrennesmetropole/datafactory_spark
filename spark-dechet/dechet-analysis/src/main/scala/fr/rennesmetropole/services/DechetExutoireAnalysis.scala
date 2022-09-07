package fr.rennesmetropole.services


import fr.rennesmetropole.services.ImportDechet.createEmptyExutoireDataFrame
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.{show,logger}
import org.apache.spark.sql._

import java.sql.Timestamp
import java.time.Instant
//import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object DechetExutoireAnalysis {
  val frTZ = java.time.ZoneId.of("Europe/Paris")
  var now = Timestamp.from(java.time.ZonedDateTime.now(frTZ).withNano(0).toInstant)
  def setNow(now_test:Instant):Unit ={
    if(Utils.envVar("TEST_MODE") != "False"){
      println("NOW TEST")
      this.now = Timestamp.from(now_test)
      println(now)
    }else {
      now = Timestamp.from(java.time.ZonedDateTime.now(frTZ).withNano(0).toInstant)
    }
  }
  /**
   * Fonction qui permet de créer un dataframe avec les donnée
   *
   * @param spark : la session spark
   * @param df    : le dataframe à traiter
   * @return
   */
  def ExecuteDechetExutoireAnalysis(spark: SparkSession, df_raw: DataFrame, SYSDATE: String, reprise: String): DataFrame = {
    if(!df_raw.head(1).isEmpty){
      try{
        val dateMesure = (date:String,immat:String,distance:String) =>{ changeDateToTz(date,immat,distance) }
        val udfReformat = udf(dateMesure)
        val valDistance = (valeur:String) =>{ distanceDouble(valeur) }
        val udfDouble = udf(valDistance)
        val df_select = df_raw.select("immat", "date_Service_Vehic", "description_Tournee", "km_Realise", "no_Bon", "lot", "service", "nom_Rech_Lieu_De_Vidage", "multiples_Lignes", "cle_Unique_Ligne_Ticket")
        var df_renamed = df_select
          .withColumnRenamed("immat", "code_immat")
          .withColumnRenamed("date_Service_Vehic", "date_mesure")
          .withColumn("date_mesure", udfReformat(col("date_mesure"),col("code_immat"),col("km_Realise")))
          .withColumnRenamed("description_Tournee", "code_tournee")
          .withColumnRenamed("km_Realise", "distance")   
          .withColumn("distance", udfDouble(col("distance"))) 
          .withColumnRenamed("no_Bon", "no_bon")      
          .withColumnRenamed("lot", "secteur")
          .withColumnRenamed("service", "nb_bac")
          .withColumnRenamed("nom_Rech_Lieu_De_Vidage", "localisation_vidage")
          .withColumnRenamed("multiples_Lignes", "poids") 
          .withColumnRenamed("cle_Unique_Ligne_Ticket", "type_vehicule")
          .withColumn("date_crea",lit(now).cast(TimestampType))
          .withColumn("date_modif",lit(null).cast(TimestampType))
        var df = df_renamed.select("date_mesure").withColumn("date_format",date_format(col("date_mesure"),"yyyy-MM")).withColumn("date_format_max",date_format(col("date_mesure"),"yyyy-MM"))

        show(df_renamed,"df_renamed")
        show(df,"df")
        var df_filter = df_renamed
        if(reprise != "reprise"){
          println("filtrage des données en cours")
          //Regle d'alimentation: Filtre Insertion nouvelle mesure du mois
          df_filter = df_renamed.filter(date_format(col("date_mesure"),"yyyy-MM") === df_renamed.select(functions.max( date_format(col("date_mesure"),"yyyy-MM"))).first().get(0))
        }

        show(df_filter,"df_filter")
        //Regle d'alimentation: Filtre Pesee embarque non vide
        val df_tri = df_filter.filter(col("localisation_vidage") =!= "PESEE EMBARQUEE" || col("localisation_vidage").isNull )
        show(df_tri,"df_tri")
        //Dédoublonnage pour reprise sur fac_exutoire2
        val df_tri2 = df_tri.where(col("no_bon").isNotNull)
        show(df_tri2,"df_tri2")

        val df_tri3 = df_tri2.select("code_immat", "date_mesure", "code_tournee", "distance",/*  "no_bon", */  "secteur", "nb_bac", "localisation_vidage", "poids", "type_vehicule","date_crea","date_modif")
        show(df_tri3,"df_tri3")
        df_tri3
      }catch{
        case e:Throwable =>
        logger.error("ERROR durant l'enrichissement ExecuteDechetExutoireAnalysis()\n" +e)
          createEmptyExutoireDataFrame(spark)
      }
    }else {
      createEmptyExutoireDataFrame(spark)
    }
  }

/*********    UDF   *********/

  def mergeToTimestamp(date:String,heure:String): String = {
    try{
      val date_ts = date.substring(0,4)+ "-" +date.substring(4,6)+ "-" +date.substring(6,8)
      val heure_ts = heure.substring(0,2)+ ":" +heure.substring(2,4)+ ":" +heure.substring(4,6)
      val frTZ = java.time.ZoneId.of("Europe/Paris")
      return date_ts + " " + heure_ts + java.time.ZonedDateTime.now(frTZ).getOffset
    }catch {
      case e: Throwable => {
        logger.error("Echec de mergeToTimestamp  for date: " + date + " heure: "+heure +" trace :" + e)
        return "0000-00-00 00:00:00+01"
       
      }
    }
  }

  def typePuce( code:String): String = {
    if(code == null){
      "Inconnu"
    }else {
      "Connu"
    }
  }

  def typeFrequence(type_puce:String,frequence_om:Integer,frequence_cs:Float): Float = {
    if( type_puce.contains(" CS ")){
      frequence_cs
    }else if (type_puce.contains(" OM ")){
      frequence_om.toFloat
    }else {
      null.asInstanceOf[Float]
    }
  }

  def changeDateToTz(date:String,immat:String,distance:String): String = {
    // Input: 13/01/2022  ==>  Output: 2020-01-13 00:00:00
    // Input: 1/1/22 (dd/mm/yyyy) ==>  Output: 2022-01-01 00:00:00
    // Input: 2020-01-13 00:00:00  ==>  Output: 2020-01-13 00:00:00
    val dateSplit = date.split("/")
    if(dateSplit.length != 1) {
      val annee = if(dateSplit(2).length == 2)"20"+dateSplit(2) else dateSplit(2)
      val mois = if(dateSplit(0).length == 1)"0"+dateSplit(0) else dateSplit(0)
      val day = if(dateSplit(1).length == 1)"0"+dateSplit(1) else dateSplit(1)
      annee + "-" + mois + "-" + day + " 00:00:00"
    }else {
      date
    }
  }

  def distanceDouble(valeur:String): Double ={
    try {
      if(valeur.contains(',')){
        valeur.replace(',','.').toDouble
      }else {
        valeur.toDouble
      }          
    }catch{
      case e : Throwable => 
        null.asInstanceOf[Double]
    }
  }
 
}