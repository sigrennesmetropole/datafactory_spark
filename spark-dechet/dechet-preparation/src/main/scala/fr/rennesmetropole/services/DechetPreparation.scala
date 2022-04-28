package fr.rennesmetropole.services

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}

object DechetPreparation {
  val logger = Logger(getClass.getName)


  /**
   * Fonction qui permet de créer un dataframe avec les donnée
   *
   * @param spark : la session spark
   * @param df    : le dataframe à traiter
   * @return
   */
  def ExecuteDechetPreparation(spark: SparkSession, df_raw: DataFrame,nameEnv:String): DataFrame = {
    var df_imported = df_raw
    //récupération des paramètres depuis le fichier de conf
    val(mapNbtoName,mapNametoParam) = Utils.getMap(spark,nameEnv)
    if(Utils.tableVar(nameEnv,"header") == "False"){
      //Renommage des colonnes
     df_imported = Utils.renommageColonnes(spark,df_raw,mapNbtoName)
    }
    
    // cast des types des colonnes
    println("DATAFRAME CAST")
    val df_cast = Utils.castDF(spark,df_imported,nameEnv,mapNametoParam) 
    val sortedList =  mapNbtoName.toSeq.sortWith((s1,s2) => Utils.sort(s1._1,s2._1))
    val values = sortedList.unzip._2
    val df_triee = df_cast.select(values.head, values.tail:_*)  // permet de reorganiser les colonnes de notre dataframe pour qu'il colle au schema initial
    df_triee
  }
 
}