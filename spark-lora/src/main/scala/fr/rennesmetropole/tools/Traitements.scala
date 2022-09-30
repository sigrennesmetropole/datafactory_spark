package fr.rennesmetropole.tools

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.WrappedArray

object Traitements {
  val logger = Logger(getClass.getName)

  val schema = StructType(
    List(
      StructField("id", StringType, false),
      StructField("deveui", StringType, false),
      StructField("timestamp", StringType, false),
      StructField("name", StringType, false),
      StructField("value", FloatType)
    )
  )


  def flattenDataFrame(raw_df: DataFrame, columns: Seq[String]): DataFrame = {
    //Création de la liste qui va servir pour renommer les colonnes
    var nouveauxNom = flattenSchema(raw_df.schema).mkString(",").replace(".","_").split(",") // On remplace les '.' par des '-' sinon cela va poser problème lors du renommage
    var flatten_df = raw_df.select(flattenSchema(raw_df.schema):_*) //permet de selectionner que les valeurs a la fin des chemins
    val df = flatten_df.toDF(nouveauxNom:_*)
    println("SCHEMA APRES RENOMMAGE")
    //df.printSchema()
    val allColumns = Seq("deveui","timestamp")++columns
    println(allColumns.mkString("-"))
    val selectColumn = allColumns.intersect(df.columns)
    val df2 = df.select(selectColumn.head, selectColumn.tail:_*) // car select accepte select(col: String, cols: String*): DataFrame, ou select(cols: Column*): DataFrame donc on ne peut pas mettre juste 'columns:_*'
    df2
  }
  /**
   * Fonction récursive qui permet de mettre a plat (sur un niveau des df imbriqué) n'importe quelle dataframe venant de JSON
   *  https://stackoverflow.com/questions/37471346/automatically-and-elegantly-flatten-dataframe-in-spark-sql
   */
  def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)
      f match {
        case StructField(_, struct:StructType, _, _) => flattenSchema(struct, colName)
        case StructField(_, ArrayType(x :StructType, _), _, _) => flattenSchema(x, colName)
        case StructField(_, ArrayType(_, _), _, _) => Array(col(colName))
        case _ => Array(col(colName))
      }
    })
  }

  /**
   *
   * Le traitement_general a pour but de prendre des dataframes issue de capteurs
   * et de les mettres sous en un certain schéma
   *
   * @param df : dataframe qui va être traité
   * @param deveui : deveui du capteur qui va être traité
   * @param spark : session spark
   * @param param : tableau contenant tout les paramètre nécessaire au traitement des données du capteur
   *
   * param[0] : nom_global_cle => séquence contenant le(s) chemin(s) vers le(s) champ(s) a mettre dans la colonne 'nom', nom qui sera présent sur toutes les lignes peut importe les valeurs, le nom du champ sera pris comme valeur
   *
   * param[1] : nom_global_valeur => séquence contenant le(s) chemin(s) vers le(s) champ(s) a mettre dans la colonne 'nom', nom qui sera présent sur toutes les lignes peut importe les valeurs, la valeur associé a ce champ sera pris comme valeur
   *
   * param[2] : nom_unique_cle => séquence contenant le(s) chemin(s) vers le(s) champ(s) a mettre dans la colonne 'nom', nom qui est asssocié a une valeur unique (ex:'temperature 1':25), le nom du champ sera pris comme valeur
   *
   * param[3] : nom_unique_valeur => séquence contenant le(s) chemin(s) vers le(s) champ(s) a mettre dans la colonne 'nom', nom qui est asssocié a une valeur unique (ex:'temperature 1':25), la valeur associé a ce champ sera pris comme valeur
   *
   * param[4] : nom_donnee_cle => séquence contenant le(s) chemin(s) vers le(s) champ(s) a mettre dans la colonne 'value', le nom du champ sera pris comme valeur
   *
   * param[5] : nom_donnee_valeur => séquence contenant le(s) chemin(s) vers le(s) champ(s) a mettre dans la colonne 'value', la valeur associé au champ sera pris comme valeur
   *
   * param[6] : nom_global_User => séquence contenant UN seul string qui est un nom a mettre dans la colonne 'nom', ce nom sera mis comme le param[0-1] sur toutes les lignes du DataFrame
   *
   * param[7] : columns => séquence contenant tout les chemins vers les champs selectionner
   */
  def traitement_pre_traitement (df: DataFrame, deveui: String, spark: SparkSession, param: Array[Seq[String]], df_step: DataFrame): DataFrame = {

    println("Traitement du capteur : " + deveui)
    if (!df.head(1).isEmpty && !param(7).isEmpty ) {
      println("DataFrame non vide, commencement du traitement...")
      val df_data = df.select( col("deveui"),col("timestamp"), col("data.*"))
      val flat = flattenDataFrame(df_data,param(7))
      return traitement_general(spark,flat,param(0),param(1),param(2),param(3),param(4),param(5),param(6), df_step.filter(col("deveui") === deveui))
    }
    else {
      println("DataFrame vide, aucune donnee, skip du traitement Adeunis_RF_Lorawan_Temp...")
    }
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  /**
   * Permet de traiter les trames et d'en extraire les données selon les paramètres
   *
   */
  def traitement_general(spark: SparkSession, df_flat: DataFrame, nom_global_cle: Seq[String], nom_global_valeur: Seq[String], nom_unique_cle: Seq[String], nom_unique_valeur: Seq[String], nom_donnee_cle: Seq[String], nom_donnee_valeur: Seq[String], nom_global_User: Seq[String], df_step: DataFrame): DataFrame ={
    println("TRAITEMENT GENERAL")
    var df_regroup = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    // On regarde si les liste qui vont servir selectionner les colonnes sont vides ou non -> si vide, cela signifie que nous n'avons pas de donnée a sélectionner
    if(!nom_donnee_cle.isEmpty && !nom_donnee_valeur.isEmpty){
      //TODO

    }else if(nom_donnee_cle.isEmpty && !nom_donnee_valeur.isEmpty){
      val selectedColumn = nom_donnee_valeur.intersect(df_flat.columns)
      if(!selectedColumn.isEmpty) {
        //df_regroup = df_flat.withColumn("values", array(selectedColumn.head, selectedColumn.tail: _*))
        var tmp_df: DataFrame = df_flat;
        selectedColumn.foreach( colName => {
          if(df_flat.schema(colName).dataType.isInstanceOf[ArrayType]
            && !df_step.filter(col("dataparameter") === colName).isEmpty) {
            val splitted = df_flat.select(col("deveui"), col("timestamp"), posexplode(col(colName)))
              .withColumn("generatedTimestamp", Utils.historicalTimestampUDF()(col("timestamp"),
                col("pos"), lit(df_step.filter(col("dataparameter") === colName).first().getAs[Int]("step"))))
              .select(col("deveui"), col("generatedTimestamp") as "timestamp", col("col") as colName )
            tmp_df = tmp_df.drop(colName)
            tmp_df = tmp_df.join(splitted, Seq("deveui", "timestamp"))
              .unionByName(splitted.join(tmp_df, Seq("deveui", "timestamp"),"anti"), allowMissingColumns=true)
          }
        })
        df_regroup = tmp_df.withColumn("values", array(selectedColumn.head, selectedColumn.tail: _*))
      }

    }else if(!nom_donnee_cle.isEmpty && nom_donnee_valeur.isEmpty){
      //TODO

    }else {
      throw new Error("Aucune valeur a selectionner renseignee")
    }

    //On met le nom qui vont être associer a valeurs mise juste avant, les noms qui sont propres a chaque jeux de valeurs (ex temperature 1, temperature2,...)
     var df_preExplode = df_regroup
    if(! nom_unique_cle.isEmpty && !nom_unique_valeur.isEmpty){
      //TODO

    }else if(nom_unique_cle.isEmpty && !nom_unique_valeur.isEmpty){
      val selectedColumn = nom_unique_valeur.intersect(df_flat.columns)
      if(!selectedColumn.isEmpty) {
        df_preExplode = df_regroup.withColumn("name", array(selectedColumn.head, selectedColumn.tail: _*))
      }

    }else if(!nom_unique_cle.isEmpty && nom_unique_valeur.isEmpty){
      df_preExplode = df_regroup.withColumn("name", (typedLit(nom_unique_cle)))

    }else {
      df_preExplode = df_regroup.withColumn("name", array(lit(""),lit("")))
    }
    val flattening = (arr:WrappedArray[_]) =>{flattenStringArrays(arr) }
    val udfFlat = udf(flattening)
    val df_preExplodeFlat = df_preExplode.withColumn("name",udfFlat(col("name"))).withColumn("values",udfFlat(col("values")))
    //Une fois toutes les données mise à plat on associe les colonnes 'names' et 'values' pour pouvoir les séparer en plusieurs lignes mais en gardant une cohérence entre les données
    val df_group = df_preExplodeFlat.withColumn("data", arrays_zip(col("name"), col("values")))

    //TODO Faire en sorte que l'on puisse enlever l'existence d'array dans des Arrays qui pose problèle par la suite lors du EXPLODE

    //Une fois les colonnes associé on peut les séparé les colonnes qui contiennent plusieurs valeurs en plusieurs lignes tout en gardant une cohérence et en gardant les lignes qui sont nul avec explode_outer
    val df_explode = df_group.withColumn("data",explode_outer(col("data")))

    //La séparation des données sur plusieurs ligne est faite mais elle sont dans une seul colonne, on sépare donc les données en plusieurs colonnes
    var df_explode2 = df_explode.drop("name","values").select("*","data.*")

    df_explode2 = df_explode2.filter(col("values") =!= "null")
    var df_AllNameFinal = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    if(!nom_global_cle.isEmpty && !nom_global_valeur.isEmpty){
      val colNames = nom_global_valeur.map(name => col(name))
      val df_AllName = df_explode2.withColumn("names1", concat_ws(":", typedLit(nom_global_cle)))
      val df_AllName2 = df_AllName.withColumn("names2", concat_ws(":", colNames:_*))
      df_AllNameFinal = df_AllName2.withColumn("name", concat_ws(":", col("names1"), col("names2"), col("name"))).drop("names1","names2")

    }else if(nom_global_cle.isEmpty && !nom_global_valeur.isEmpty){
      val colNames = nom_global_valeur.map(name => col(name))
      val df_AllName = df_explode2.withColumn("names1", concat_ws(":", colNames:_*))
      df_AllNameFinal = df_AllName.withColumn("name", concat_ws(":", col("names1"), col("name"))).drop("names1")

    }else if(!nom_global_cle.isEmpty && nom_global_valeur.isEmpty){
      df_AllNameFinal = df_explode2.withColumn("name", concat_ws(":", typedLit(nom_global_cle), col("name")))

    }else {
      df_AllNameFinal = df_explode2
    }

    if (!nom_global_User.isEmpty){
      df_AllNameFinal = df_AllNameFinal.withColumn("name", concat_ws(":", lit(nom_global_User(0)), col("name")))
    }

    //On passe les données de la colonne values de Array à int et on renomme la colonne pour coller au schéma
    var df_clean = df_AllNameFinal
    //On vérifie si la colonne values est un array, si oui on extrait sa valeur pour avoir juste la valeur
    if(df_AllNameFinal.schema("values").dataType.isInstanceOf[ArrayType]){
      df_clean = df_AllNameFinal.select(col("deveui"), col("timestamp"), col("name"), col("values").getItem(0) as "value")
    }else {df_clean = df_AllNameFinal.withColumnRenamed("values","value")}
    //On ajoute la date d'insertion dans une nouvelle colonne
    val date = java.time.OffsetDateTime.now().toString
    println("DATE INSERTED: " + date)
    val df_withDate = df_clean.withColumn("insertedDate", lit(date))

    //On fini par ajouter la colonne id qui va servir de clé primaire
    val df_withId = df_withDate.withColumn("id", concat_ws(" | ", col("deveui"), col("timestamp"), col("name")))

    val onlyValue = (str:Any) =>{extractNumberFromString(str) }
    val udfValue = udf(onlyValue)
    val df_nullfiltered = df_withId.filter(col("value").isNotNull)
    val df_final = df_nullfiltered.select(col("deveui"),col("timestamp"),col("name"),udfValue(col("value")) as "value",col("insertedDate"),col("id")).withColumnRenamed("timestamp","tramedate") //L'UDF permet de ne prendre que la valeur (le nombre) et pas l'unité si elle est présente
    println("-- df traitement_general--")
    df_final.show(10, false)

    println("FIN TRAITEMENT GENERAL")
    df_final
  }

  /**
   * UDF permettant de prendre une colonne de type tableau et de l'applatir (suppression de tableau imbriqué)
   * @param arr:WrappedArray[_] Array contenue dans la colonne où l'on souhaite appliqué l'UDF
   * @return Array[String] un tableau d'une dimension
   */
  def flattenStringArrays(arr: WrappedArray[_]): Array[String] = {
        var res = arr.mkString(",")
        res = res.replace(", ",",")
        var toRemove = "(".toSet
        res = res.filterNot(toRemove)
        toRemove = ")".toSet
        res = res.filterNot(toRemove)
        res = res.replace("[ ","")
        res = res.replace(" ]","")
        toRemove = "[".toSet
        res = res.filterNot(toRemove)
        toRemove = "]".toSet
        res = res.filterNot(toRemove)
        res = res.replace(" WrappedArray","")
        res = res.replace("WrappedArray","")
        val array = res.split(",")
        array
  }

  /**
   * UDF permettant de prendre une colonne, d'en extraire la partie algébrique, puis de la convertir en Float
   *
   */
  def extractNumberFromString(str: Any): Float = {
    //println(" STRING  =>   "+ str)

    if(str != null ){
    val string = str.toString
    if(!string.isEmpty){
    val valeur = string.replaceAll("[^-.0-9]+", "");
     if(!valeur.isEmpty){
    //println(" FLOAT  =>   "+ valeur.toFloat)
    valeur.toFloat
     }else return "0.0".toFloat //TODO
    }else return "0.0".toFloat //TODO
    }else return "0.0".toFloat //TODO
  }

}