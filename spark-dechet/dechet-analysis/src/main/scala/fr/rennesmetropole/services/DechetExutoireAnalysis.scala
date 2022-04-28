package fr.rennesmetropole.services

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.services.ImportDechet.createEmptyExutoireDataFrame
import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql._
//import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object DechetExutoireAnalysis {
  val logger = Logger(getClass.getName)

  /**
   * Fonction qui permet de créer un dataframe avec les donnée
   *
   * @param spark : la session spark
   * @param df    : le dataframe à traiter
   * @return
   */
  def ExecuteDechetExutoireAnalysis(spark: SparkSession, df_raw: DataFrame, SYSDATE: String): DataFrame = {
    if(!df_raw.head(1).isEmpty){
      try{
        val dateMesure = (date:String) =>{ changeDateToTz(date) }
        val udfReformat = udf(dateMesure)
        val valDistance = (valeur:String) =>{ distanceDouble(valeur) }
        val udfDouble = udf(valDistance)
        val df_select = df_raw.select("immat", "dateServiceVehic", "codeTournee", "kmRealise", "noBon", "lot", "service", "nomRechLieuDeVidage", "multiplesLignes", "cleUniqueLigneTicket")
        df_select.show(10)
        val df_renamed = df_raw
          .withColumnRenamed("immat", "code_immat")
          .withColumnRenamed("dateServiceVehic", "date_mesure")
          .withColumn("date_mesure", udfReformat(col("date_mesure")))
          .withColumnRenamed("codeTournee", "code_tournee")
          .withColumnRenamed("kmRealise", "distance")   
          .withColumn("distance", udfDouble(col("distance"))) 
          .withColumnRenamed("noBon", "no_bon")      
          .withColumnRenamed("lot", "secteur")
          .withColumnRenamed("service", "nb_bac") 
          .withColumn("nb_bac", udfDouble(col("nb_bac")))
          .withColumnRenamed("nomRechLieuDeVidage", "localisation_vidage")
          .withColumnRenamed("multiplesLignes", "poids") 
          .withColumnRenamed("cleUniqueLigneTicket", "type_vehicule")
          .withColumn("date_crea",lit(Utils.dateTimestampWithZone("YYYY-MM-dd HH:mm:ss")))
          .withColumn("date_modif",lit(null).cast(TimestampType))
        
        df_renamed.show(10)

        //Regle d'alimentation: Filtre Insertion nouvelle mesure du mois
        val df_filter = df_renamed.filter(date_format(col("date_mesure"),"yyyy-MM") === df_renamed.select(functions.max( date_format(col("date_mesure"),"yyyy-MM"))).first().get(0))
        //Regle d'alimentation: Filtre Pesee embarque non vide
        val df_tri = df_filter.filter(col("localisation_vidage") =!= "PESEE EMBARQUEE" || col("localisation_vidage").isNull )
        //Dédoublonnage pour reprise sur fac_exutoire2
        val df_tri2 = df_tri.where(col("no_bon").isNotNull)

        df_tri2.select("code_immat", "date_mesure", "code_tournee", "distance",/*  "no_bon", */  "secteur", "nb_bac", "localisation_vidage", "poids", "type_vehicule","date_crea","date_modif")
      }catch{
        case e:Throwable => 
        println("ERROR durant l'enrichissement ExecuteDechetExutoireAnalysis()\n" +e)
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

  def changeDateToTz(date:String): String = {
    // Input: 13/01/20  ==>  Output: 2020-01-13 00:00:00
    val dateSplit = date.split("/")

    if(dateSplit(2).length<=2){
      val time = "20" + dateSplit(2) + "-" + dateSplit(1) + "-" + dateSplit(0) + " 00:00:00"
      time
    }else{
      val time = dateSplit(2) + "-" + dateSplit(1) + "-" + dateSplit(0) + " 00:00:00"
      time
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