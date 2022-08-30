package fr.rennesmetropole.services

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.tools.{UDF, Utils}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.functions.{col, lit, udf, when,length}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession,Row}

object DechetRefPreparation {
  val logger = Logger(getClass.getName)


  /**
   * Fonction qui permet de creer un dataframe avec les donnees referentiels
   *
   * @param spark : la session spark
   * @param df    : le dataframe à traiter
   * @return
   */
  def ExecuteDechetRefPreparation(spark: SparkSession, df_raw_producteur: DataFrame, nameEnv:String): DataFrame = {
    var df_imported = df_raw_producteur
    val schema = Utils.getSchema(nameEnv)
    var df_typed = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
    println("SCHEMA DF TYPED")
    df_typed.printSchema
    if(Utils.tableVar(nameEnv,"header") == "False"){ // On regarde si la table possede un header
      //recuperation des parametres depuis le fichier de conf
      println("getMap" + nameEnv)
      val(mapNbtoName,mapNametoParam) = Utils.getMap(spark,nameEnv)
      print(" map :"+ mapNbtoName.mkString(","))
      df_imported = Utils.renommageColonnes(spark,df_raw_producteur,mapNbtoName)
      println("renommage : ")
      df_imported.show(false)
    }
    else {
      // Application d'un filtre sur les headers pour enlever les caracteres non souhaite
      df_imported = Utils.regexCharSpe(spark, df_raw_producteur)
      println("Apres filtre header")
      df_imported.show()
      // On met tous les caracteres en minuscule
      df_imported = Utils.lowerCaseAllHeader(spark, df_imported)
    }

    // try{
    //   df_imported = df_imported.unionByName(df_typed)
    // }
    // catch {
    //    case e : Throwable =>
    //   println("ERROR : les donnees en entree n'ont pas les bonnes colonnes : " + e)
    //   df_typed
    // }
      val df_prepared = nameEnv match {
      case "tableProducteur" => preparationProducteur(df_imported, df_typed) 
      case "tableRecipient" => preparationRecipent(df_imported, df_typed) 
      }
      println("DONNEES PREPARE")
      df_prepared.show
      df_prepared
  }

/**
 * Fonction qui permet de preparer les donnees des producteurs
 * @param df Dataframe qui contient les donnees qui vont être traite
 */
def preparationProducteur(df: DataFrame, df_typed :DataFrame): DataFrame = {
  try {
    println("DF PREPARATION PRODUCTEUR")
    val df_prod =  df.withColumn("code_producteur",new Column(AssertNotNull(col("code_producteur").cast(IntegerType).expr)))
      .withColumn("rva_adr",col("rva_adr").cast(IntegerType))
      .withColumn("code_insee",col("code_insee").cast(IntegerType))
      .withColumn("nom_commune",new Column(AssertNotNull(col("nom_commune").cast(StringType).expr)))
      .withColumn("type_producteur",col("type_producteur").cast(StringType))
      .withColumn("activite",new Column(AssertNotNull(col("activite").cast(StringType).expr)))
      .withColumn("longitude",col("longitude").cast(DoubleType))
      .withColumn("latitude",col("latitude").cast(DoubleType))
      .withColumn("date_photo",new Column(AssertNotNull(col("date_photo").cast(StringType).expr)))
      df_prod.show()
      println("verification des tailles...")
      val test_longueur = df_prod.select("nom_commune", "type_producteur", "activite")
      .filter(length(col("nom_commune"))>500 || length(col("type_producteur"))>100 || length(col("activite"))>500)
      test_longueur.show()
      if(!test_longueur.isEmpty){
        throw new Exception("La taille des valeurs dans les colonnes n'est pas respectee" )
      }
      df_prod
  }
  catch {
      case e : Throwable =>
      println("ERROR lors de la preparation du referentiel producteur : " + e)
      df_typed
    }
 
}

/**
 * Fonction qui permet de preparer les donnees des recipients
 * @param df Dataframe qui contient les donnees qui vont être traite
 */
def preparationRecipent(df: DataFrame, df_typed :DataFrame): DataFrame = {
  try {
    var df_temp = df
    println("TRY PREPARATION BAC")
      val toDouble = udf((s:String) =>UDF.udfToDouble(s))
      val toInt = udf((s:String) =>UDF.udfToInt(s))
      if(df.columns.contains("type_de_recipient")){
        df_temp = df.withColumnRenamed("type_de_recipient","type_recipient")
      }
      val df_bac = df_temp.withColumn("code_producteur", new Column(AssertNotNull(col("code_producteur").cast(IntegerType).expr)))
          .withColumn("categorie_recipient", new Column(AssertNotNull(col("categorie_recipient").cast(StringType).expr)))
          .withColumn("type_recipient", new Column(AssertNotNull(col("type_recipient").cast(StringType).expr)))
          .withColumn("litrage_recipient", new Column(AssertNotNull(col("litrage_recipient").cast(IntegerType).expr)))
          .withColumn("code_puce", new Column(AssertNotNull(col("code_puce").cast(StringType).expr)))
          .withColumn("frequence_om",when(col("frequence_om").isNotNull,
            toInt(col("frequence_om")).cast(IntegerType)).otherwise(lit(0).cast(IntegerType)))
          //.withColumn("frequence_cs",col("frequence_cs").cast(DoubleType))
          .withColumn("frequence_cs",when(col("frequence_cs").isNotNull,
            toDouble(col("frequence_cs")).cast(DoubleType)).otherwise(lit(0).cast(DoubleType))) //TO DO -> expected:1.00 but was: 1.0
          .withColumn("date_photo",new Column(AssertNotNull(col("date_photo").cast(StringType).expr)))
          //.drop(col("type_de_recipient"))
          df_bac.show()
          println("verification des tailles...")
          val test_longueur = df_bac.select("code_puce", "categorie_recipient", "type_recipient")
          .filter(length(col("code_puce"))>10 || length(col("categorie_recipient"))>100 || length(col("type_recipient"))>500)
          test_longueur.show()
          if(!test_longueur.isEmpty){
            throw new Exception("La taille des valeurs dans les colonnes n'est pas respectee" )
          }
          df_bac
  }
    catch {
      case e : Throwable =>
      println("ERROR lors de la preparation du referentiel bac : " + e)
      df_typed
    }
 
}
 
}