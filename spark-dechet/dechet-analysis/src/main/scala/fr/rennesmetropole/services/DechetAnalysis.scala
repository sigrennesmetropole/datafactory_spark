package fr.rennesmetropole.services

import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.{log, show}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.sql.Timestamp
import java.time.Instant
import java.util.Properties
import org.apache.spark.sql.expressions.Window

object DechetAnalysis {
  val frTZ = java.time.ZoneId.of("Europe/Paris")
  var now = Timestamp.from(java.time.ZonedDateTime.now(frTZ).withNano(0).toInstant)

  
  def setNow(now_test:Instant):Unit ={
    if(Utils.envVar("TEST_MODE") != "False"){
      log("NOW TEST")
      this.now = Timestamp.from(now_test)
      log(now)
    }else {
       now = Timestamp.from(java.time.ZonedDateTime.now(frTZ).withNano(0).toInstant)
    }
  }
  /**
   * Fonction qui permet de creer un dataframe avec les donnee
   *
   * @param spark : la session spark
   * @param df    : le dataframe a traiter
   * @return
   */
  def ExecuteDechetAnalysis_Collecte(spark: SparkSession, df_raw: DataFrame, SYSDATE: String, df_lastestBac: DataFrame): DataFrame = {
    val schema = StructType(
          List(
            StructField("date_mesure", StringType, true),
            StructField("id_bac", StringType, false),
            StructField("code_puce", StringType, false),
            StructField("code_tournee", StringType, false),
            StructField("code_immat", StringType, false),
            StructField("poids", StringType, false),
            StructField("poids_corr", StringType, false),
            StructField("latitude", StringType, false),
            StructField("longitude", StringType, false),
            StructField("date_crea", TimestampType, false),
            StructField("date_modif", TimestampType, false),
            StructField("year", StringType, false),
            StructField("month", StringType, false),
            StructField("day", StringType, false)
          )
        )
    log("Dechet Analsyis")
    if(!df_raw.head(1).isEmpty){
      try{
        val df_lastestBacFiltered = df_lastestBac.select("id_bac", "code_puce","date_debut","date_fin","categorie_recipient")
        val dfJoined = df_raw.join(df_lastestBacFiltered,
          df_raw("Code_puce") <=> df_lastestBacFiltered("code_puce") &&
            Utils.timestampWithoutZone()(col("Date_de_la_levee"),col("Heure_de_la_levee")) >= df_lastestBacFiltered("date_debut")
            && (df_lastestBacFiltered("date_fin").isNull
            || Utils.timestampWithoutZone()(col("Date_de_la_levee"),col("Heure_de_la_levee")) <= df_lastestBacFiltered("date_fin")), "left")
          .drop(df_lastestBacFiltered("code_puce"))

         //dfJoined.show(false)

        var df_withTimestamp = dfJoined
          .withColumn("id_bac",
            when(col("id_bac").isNull, lit(df_lastestBacFiltered.filter(df_lastestBacFiltered("code_puce") === "INCONNU").first.getAs("id_bac")))
          .otherwise(col("id_bac")))
          .withColumn("date_mesure",Utils.timestampWithZoneUdf()(col("Date_de_la_levee"),col("Heure_de_la_levee")).cast(TimestampType))
          .withColumn("code_tournee",col("Tournee"))
          .withColumn("code_immat",col("Immatriculation"))
          .withColumn("poids",col("Pesee_net"))
          .withColumn("poids_corr",lit(null).cast(FloatType))
          .withColumn("date_crea", lit(now).cast(TimestampType))
          .withColumn("date_modif",lit(null).cast(TimestampType))

        df_withTimestamp = df_withTimestamp.withColumn("id_bac_by_date",when(
            col("date_mesure") >= col("date_debut")
              && (col("date_fin").isNull
              || col("date_mesure") <= col("date_fin")), df_withTimestamp("id_bac") )
          .otherwise(lit(df_lastestBacFiltered.filter(df_lastestBacFiltered("code_puce") === "INCONNU").first.getAs("id_bac"))))
        show(df_withTimestamp,"df_withTimestamp")
       /* df_withTimestamp = df_withTimestamp.drop("id_bac").withColumnRenamed("id_bac_by_date","id_bac")
          .filter(col("date_mesure") > col("date_fin")
            || col("date_mesure") < col("date_debut"))*/
       //Partie redressement données
        val df_poids_corr = redressement_donne(df_withTimestamp,spark,SYSDATE)
        df_poids_corr.select("date_mesure","code_puce", "id_bac","code_tournee","code_immat","poids","poids_corr","latitude","longitude","date_crea","date_modif","type_flux")
      }catch{
        case e:Throwable =>
        log("ERROR durant l'enrichissement ExecuteDechetAnalysis()\n" + e)
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
      }
    }else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
    }
  }
  def redressement_donne(df_raw: DataFrame, spark: SparkSession,date:String): DataFrame = {
  // on identifie les type de flux
    var df_withTypeFlux = df_raw.withColumn("type_flux",Utils.type_flux_UDF("pre_flux")(df_raw("categorie_recipient"),df_raw("code_tournee"),df_raw("code_immat")))
    show(df_withTypeFlux,"df_withTypeFlux")

    //On récupère la liste des code_immat qui n'ont pas de catégorie récipient et pas de code_tourne mais qui sont déductible car code_immat non null
    var df_codeTourneeNull_and_CodeImmatNotNull = df_withTypeFlux.select("type_flux","code_immat").where(col("type_flux")===lit("Inconnu_connu")).select("code_immat").distinct()
    show(df_codeTourneeNull_and_CodeImmatNotNull,"df_codeTourneeNull_and_CodeImmatNotNull")

    //On récupère les code_immat qui corresponde a des catégorie_recipient null pour determiner le flux le plus probable (flux que le camion a le plus récupéré)
    //regroupe les différent type flux par code_immat
    var df_withTypeFlux_temp = df_withTypeFlux.join(df_codeTourneeNull_and_CodeImmatNotNull,Seq("code_immat"),"inner")
      .select("type_flux", "code_immat","code_tournee", "categorie_recipient","date_mesure")
      .filter(col("code_tournee").isNull && col("code_immat").isNotNull)
    val df_min_max = df_withTypeFlux_temp.groupBy("code_immat").agg(max("date_mesure") as "date_max", min("date_mesure") as "date_min")
    df_withTypeFlux_temp = df_withTypeFlux_temp.join(df_min_max,Seq("code_immat"),"inner")
    df_withTypeFlux_temp= df_withTypeFlux_temp.withColumn("type_flux",Utils.type_flux_UDF("intra_flux")(col("type_flux"),col("categorie_recipient"),col("date_mesure"),col("date_min"),col("date_max"))) //On affecte a tout les bac pas rattache un type flux correspondant a son code_immat
      .groupBy("type_flux", "code_immat","date_max","date_min").count().orderBy(desc("count"))
      .withColumnRenamed("type_flux","categorie_recipient_moyen")
    import org.apache.spark.sql.expressions.Window
    //Utilisation de l'outil "window" pour garder seulement le type_flux avec le maximum d'occurence par code_immat
    val windowSpec  = Window.partitionBy( "code_immat")
    df_withTypeFlux_temp =  df_withTypeFlux_temp.withColumn("max",max("count").over(windowSpec)).where(df_withTypeFlux_temp("count")===col("max"))//.drop("max","count")
    df_withTypeFlux_temp = df_withTypeFlux.join(df_withTypeFlux_temp,Seq("code_immat"),"left")
    //Update de la colonne de flux
    var df_withAllTypeFlux = df_withTypeFlux_temp.withColumn("type_flux_temp",Utils.type_flux_UDF("post_flux")(df_withTypeFlux_temp("type_flux"),df_withTypeFlux_temp("code_immat"),df_withTypeFlux_temp("categorie_recipient_moyen"))).drop("type_flux").withColumnRenamed("type_flux_temp","type_flux")
    show(df_withAllTypeFlux,"df_withAllTypeFlux")

    //Redressement des pesées en fonction des flux
    val df_poids_corr = df_withAllTypeFlux.withColumn("poids_corr",Utils.redressementUDF()(df_withAllTypeFlux("code_tournee"),df_withAllTypeFlux("poids"),df_withAllTypeFlux("type_flux")))
    show(df_poids_corr,"df_poids_corr")
    df_poids_corr
  }
  def redressement_donne_incorrect(df_a_redresser: DataFrame,df_support: DataFrame,df_refBac: DataFrame, spark: SparkSession,dateRef:String,date:String): DataFrame = {
    log("redressement_donne_incorrect")
    // ********** Partie gestion des moyennes des flux sur le même mois de l'année précedente **********

    // seuil corrospondant au nombre de valeur qu'il doit y avoir pour avoir un historique valide
    val seuil_historique = 6 // TODO seuil a mettre a 3 pour les tests unitaires

    //liste pour calculer les moyennes dynamiques pour les pesées à corriger avec bac rattaché
    var liste_litrage_flux = Seq(
      Row("OM140", "Bacs ordures ménagères", "120-140", 9.7),
      Row("OM180", "Bacs ordures ménagères", "180", 12.4),
      Row("OM240", "Bacs ordures ménagères", "240", 17.6),
      Row("OM360", "Bacs ordures ménagères", "330-340-360", 27.6),
      Row("OM400", "Bacs ordures ménagères", "400", 22.0),
      Row("OM660", "Bacs ordures ménagères", "500-660", 46.4),
      Row("OM770", "Bacs ordures ménagères", "750-770-1000", 52.7),
      Row("CS140", "Bacs collecte sélective", "120-140", 5.2),
      Row("CS180", "Bacs collecte sélective", "180", 6.8),
      Row("CS240", "Bacs collecte sélective", "240", 7.6),
      Row("CS360", "Bacs collecte sélective", "330-340-360", 10.7),
      Row("CS660", "Bacs collecte sélective", "500-660", 21.9),
      Row("CS770", "Bacs collecte sélective", "750-770-1000", 24.1),
      Row("BIO140", "Bacs biodéchets", "120-140", 34.9),
      Row("BIO240", "Bacs biodéchets", "240", 29.1),
      Row("BIO400", "Bacs biodéchets", "400", 64.5),
      Row("VE240", "Bacs verre", "240", 46.6),
      Row("VE400", "Bacs verre", "400", 71.6),
      Row("VE770", "Bacs verre", "750-770-1000", 72.1))
    //liste pour calculer les moyennes globales même mois année - 1 sans bac rattaché
    var liste_flux = Seq(
      Row ("OMglobal", "OM", 17.8),
      Row ("CSglobal", "CS", 9.2),
      Row ("BIOglobal", "BIO", 53.5),
      Row ("VEglobal", "VE", 60.5))

    val df_join = broadcast(df_support).join(df_refBac,Seq("id_bac"),"left")
    show(df_refBac,"df_refBac")
    show(df_join,"df_join")

    import spark.sqlContext.implicits._
    //Creation du dataframe contenant la liste des donnée qui nous permet de calculer les moyennes
    val column_liste_litrage_flux = List(
      StructField("ex_type_flux", StringType, true),
      StructField("categorie", StringType, true),
      StructField("ex_litrage_recipient", StringType, true),
      StructField("moyenne_redressement", DoubleType, true)
    )
    val liste_litrage_fluxToDf = spark.createDataFrame(spark.sparkContext.parallelize(liste_litrage_flux),
      StructType(column_liste_litrage_flux)
    )
    show(liste_litrage_fluxToDf,"liste_litrage_fluxToDf")
    //calcul des moyennes pour les pesé avec bac rattaché et pour chaque type de récipient
    var df_join_litrage_flux = df_join.join(broadcast(liste_litrage_fluxToDf),col("categorie_recipient")===col("categorie"),"left")
      .select("ex_type_flux","categorie","ex_litrage_recipient","moyenne_redressement","categorie_recipient","poids","poids_corr","date_mesure","litrage_recipient")
    var df_moyenne_litrage_flux = df_join_litrage_flux.filter(
      date_format(col("date_mesure"), "yyyy-MM") === lit(dateRef) &&
        col("poids") > 0 && df_join("poids") < 250 &&
        col("ex_litrage_recipient").contains(col("litrage_recipient")))
    val moyenne_litrage_flux = df_moyenne_litrage_flux.groupBy("ex_type_flux").agg(count("*") as "count",avg("poids").as("moyenne_poids")).filter(col("count")>50)  // TODO changer le 1 en 50 si plus en phase de test
    val df_map_litrage_flux = liste_litrage_fluxToDf.join(broadcast(moyenne_litrage_flux),Seq("ex_type_flux"),"left")
      .withColumn("moyenne_poids",when(col("moyenne_poids").isNull,col("moyenne_redressement")).otherwise(col("moyenne_poids"))).select("ex_type_flux","moyenne_poids")
    //Map pour le moyennes des pesées à corriger avec bac rattaché
    val mapMoyenneBacRattache = df_map_litrage_flux.rdd.map(row => (row.getString(0) -> row.getDouble(1))).collectAsMap()
    log("mapMoyenneBacRattache : " + mapMoyenneBacRattache.mkString(" - "))

    //Creation du dataframe contenant la liste des donnée qui nous permet de calculer les moyennes
    val column_liste_flux = List(
      StructField("ex_type_flux", StringType, true),
      StructField("ex_type", StringType, true),
      StructField("moyenne_redressement", DoubleType, true)
    )
    val liste_fluxToDf = spark.createDataFrame(spark.sparkContext.parallelize(liste_flux),
      StructType(column_liste_flux)
    )
    show(liste_fluxToDf, "liste_fluxToDf")
    //calcul des moyennes pour les pesé avec bac non rattaché et pour chaque type de récipient
    val df_support_type = df_support.withColumn("type",split(col("code_tournee"),"-").getItem(2))
    val df_join_flux = liste_fluxToDf.join(broadcast(df_support_type),liste_fluxToDf("ex_type")===df_support_type("type"),"left")
    val moyenne_flux = df_join_flux.filter(date_format(col("date_mesure"), "yyyy-MM") === lit(dateRef) &&
      col("poids") > 0 && col("poids") < 250
    ).groupBy("ex_type_flux").agg(avg("poids").as("moyenne_poids"))

    val df_map_flux = liste_fluxToDf.join(broadcast(moyenne_flux),Seq("ex_type_flux"),"left")
      .withColumn("moyenne_poids",when(col("moyenne_poids").isNull,col("moyenne_redressement")).otherwise(col("moyenne_poids"))).select("ex_type_flux","moyenne_poids")
    //Map pour les moyennes  globales même mois année-1 sans bac rattaché
    val mapMoyenneSansBacRattache = df_map_flux.rdd.map(row => (row.getString(0) -> row.getDouble(1))).collectAsMap()
    log("mapMoyenneSansBacRattache : " + mapMoyenneSansBacRattache.mkString(" - "))

    //Calcul des moyennes des flux Inconnu et Autres
    val moyenne = df_support.filter(date_format(df_support("date_mesure"),"yyyy-MM")===lit(dateRef) &&
      df_support("poids")>0 && df_support("poids")<250)
      .agg(avg("poids").as("moyenne_poids"))
    //Variable pour le moyennes  globales même mois année-1 sans bac rattaché
    val mapMoyenneBacInconnu = moyenne.take(1).head(0).toString.toDouble
  log("BacInconnu moyenne :" + mapMoyenneBacInconnu)
    // ********** Partie de la mise en place de la correction du poids

    if(!dateRef.contains("2020")){
      val connectionProps = new Properties()
      val url = Utils.envVar("POSTGRES_URL")
      connectionProps.setProperty("driver", "org.postgresql.Driver")
      connectionProps.setProperty("user", Utils.envVar("POSTGRES_ACCESS_KEY"))
      connectionProps.setProperty("password", Utils.envVar("POSTGRES_SECRET_KEY"))
      // on selectionne les x dernière valeurs reçus de chaque id_bac ayant pour poids_corr le poids
      val req =s"""
                  |select * from (
                  |    select id_bac,
                  |           poids,
                  |           poids_corr,
                  |		        date_mesure,
                  |           row_number() over (partition by id_bac order by date_mesure desc) as history_value
                  |    from dwh.fac_dechets where date_mesure <= '$date') ranks
                  |where  poids=poids_corr and history_value <= $seuil_historique
                  |""".stripMargin
      val df_history = spark.read.jdbc(url, s"($req) as temp", connectionProps)
      show(df_history,"df_history")
      //dataframe avec tout les id_bac qui ont 6 valeurs historisé dont on peut se servir pour corriger
      val df_correction_valide = df_history.groupBy("id_bac").agg(avg("poids") as "avg",count("poids") as "count").filter(s"count==$seuil_historique")//.drop("count")
      show(df_correction_valide,"df_countdf_count_valide")
      println("AVERAGE = 1 ?")
      df_correction_valide.filter(col("avg")===lit("1.0") || col("avg")===lit("1")).show()
      //dataframe avec tout les id_bac qui n'ont pas 6 valeurs historisé pour corriger
      val df_correction_invalide = df_history.groupBy("id_bac").count().filter(s"count<$seuil_historique").drop("count")
      show(df_correction_invalide,"df_count_non_valide")
      //jointure entre les données a corriger de la journée et le référentiel pour les collecte dechets qui sont rattaché a des bacs
      var df_correction = df_a_redresser.where(concat_ws("-",col("year"),col("month"),col("day"))===lit(date)).join(df_refBac,Seq("id_bac"),"left") // JOINTURE pour récupérer les dechet qui sont avec des bacs
        .drop(df_refBac("code_puce")).drop(df_refBac("year")).drop(df_refBac("month")).drop(df_refBac("day")).drop(df_refBac("date_modif")).drop(df_refBac("date_crea")) // drop des colonnes en trop dû a la jointure
      show(df_a_redresser,"df_a_redresser")
      df_correction = df_correction.join(df_correction_valide,Seq("id_bac"),"left")
      df_correction = df_correction.withColumn("poids_corr",when(col("poids_corr")===lit("0.0") || col("poids_corr")===lit("0"),col("avg")).otherwise(col("poids_corr"))).drop("avg").withColumn("litrage_recipient",col("litrage_recipient").cast(StringType))
      show(df_correction,"df_correction")

      //dataframe avec le poids corrigé pour certains id_bacs (id_bac avec moins de 6 valeurs historisé
      val df_correction_non_valide = df_correction.join(df_correction_invalide,Seq("id_bac"),"left").withColumn("poids_corr",Utils.redressementCorrectionInvalideUDF(mapMoyenneBacRattache,mapMoyenneSansBacRattache,mapMoyenneBacInconnu)(col("type_flux"),col("litrage_recipient"),col("poids_corr")))
      show(df_correction_non_valide,"df_correction_non_valide")
      val df_final = df_correction_non_valide.select("date_mesure","code_puce","id_bac","code_tournee","code_immat","poids","poids_corr","latitude","longitude","date_crea","date_modif","year","month","day")
      show(df_final,"df_final")
      df_final
    }else {
      df_a_redresser
    }

  }
  def prepareIncomingProducteurDf(df_raw: DataFrame, spark: SparkSession): DataFrame = {
    try{
      val df = df_raw
        .withColumn("id_producteur",Utils.idProducteurUDF()(col("code_producteur"),col("date_photo")))
        .withColumn("code_producteur",col("code_producteur"))
        .withColumn("id_rva",col("rva_adr"))
        .withColumn("commune",col("nom_commune"))
        .withColumn("code_insee",col("code_insee"))
        .withColumn("type_producteur",col("type_producteur"))
        .withColumn("activite",col("activite"))
        .withColumn("latitude",col("latitude"))
        .withColumn("longitude",col("longitude"))
        .withColumn("date_debut",Utils.timestampWithZoneUdf()(col("date_photo"),lit("000000")).cast(TimestampType))
        .withColumn("date_fin",lit(null).cast(TimestampType))
        .withColumn("date_crea", lit(now).cast(TimestampType))
        .withColumn("date_modif",lit(null).cast(TimestampType))
      df.select("id_producteur","code_producteur","id_rva","commune","code_insee","type_producteur","activite","latitude","longitude","date_debut","date_fin","date_crea","date_modif")
    }catch{
      case e:Throwable =>
        log("ERROR durant l'enrichissement ExecuteDechetAnalysis()\n" +e)
        createEmptyProducteurDf(spark)
    }
  }

  def createEmptyProducteurDf(spark: SparkSession): DataFrame = {
    val schema = StructType(
      List(
        StructField("id_producteur", LongType, false),
        StructField("code_producteur", IntegerType, true),
        StructField("id_rva", IntegerType, true),
        StructField("commune", StringType, true),
        StructField("code_insee", IntegerType, true),
        StructField("type_producteur", StringType, true),
        StructField("activite", StringType, true),
        StructField("latitude", DoubleType, true),
        StructField("longitude", DoubleType, true),
        StructField("date_debut", TimestampType, false),
        StructField("date_fin", TimestampType, true),
        StructField("date_crea", TimestampType, false),
        StructField("date_modif", TimestampType, true),
        StructField("year", StringType, false), 
        StructField("month", StringType, false), 
        StructField("day", StringType, false)
      )
    )
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
  }

  def deletedProducteurTreatment(df_partitionedIncomingProducteur: DataFrame, datePhoto: String, df_latest: DataFrame) = {
    val dfDeleted = df_latest.join(df_partitionedIncomingProducteur,
      df_latest("code_producteur") === df_partitionedIncomingProducteur("code_producteur"), "left_anti")
      .filter(col("code_producteur").isNotNull && col("date_fin").isNull)
      .withColumn("date_fin",
        Utils.timestampWithZoneUdf()(lit(datePhoto),lit("000000")).cast(TimestampType))
      .withColumn("date_modif", lit(now).cast(TimestampType))

    show(dfDeleted,"Producteurs supprimes")

    dfDeleted
  }

  def createProducteurTreatment(df_partitionedIncomingProducteur: DataFrame, datePhoto: String, df_latest: DataFrame) = {
    val df_ToCreate = df_partitionedIncomingProducteur.join(df_latest.filter(col("date_fin").isNull),
      df_latest("code_producteur") === df_partitionedIncomingProducteur("code_producteur") , "left_anti")
      .withColumn("date_modif", lit(null).cast(TimestampType))

    show(df_ToCreate,"Nouveaux producteurs")

    df_ToCreate
  }

  def updatedProducteurTreatment(df_latestProducteur: DataFrame, df_partitionedIncomingProducteur: DataFrame, datePhoto: String) = {
    var df_updated = df_latestProducteur.filter(col("date_fin").isNull).join(broadcast(df_partitionedIncomingProducteur), df_latestProducteur("code_producteur")
      === df_partitionedIncomingProducteur("code_producteur"), "inner")
      .drop(df_partitionedIncomingProducteur("date_crea"))
      .drop(df_partitionedIncomingProducteur("date_fin"))
      .drop(df_partitionedIncomingProducteur("date_modif"))
      .drop(df_partitionedIncomingProducteur("date_debut"))
      .drop(df_partitionedIncomingProducteur("year"))
      .drop(df_partitionedIncomingProducteur("month"))
      .drop(df_partitionedIncomingProducteur("day"))
      .drop(df_partitionedIncomingProducteur("id_producteur"))
      .drop(df_latestProducteur("code_producteur"))
      .drop(df_latestProducteur("id_rva"))
      .drop(df_latestProducteur("commune"))
      .drop(df_latestProducteur("code_insee"))
      .drop(df_latestProducteur("type_producteur"))
      .drop(df_latestProducteur("activite"))
      .drop(df_latestProducteur("latitude"))
      .drop(df_latestProducteur("longitude"))
      .withColumn("date_modif", lit(now).cast(TimestampType))

    show(df_updated,"Producteurs mis a jour")

   /* val df_reactivated = df_latestProducteur.filter(col("date_fin").isNotNull).join(df_partitionedIncomingProducteur, df_latestProducteur("code_producteur")
      === df_partitionedIncomingProducteur("code_producteur"), "inner")
      .drop(df_latestProducteur("date_crea"))
      .drop(df_latestProducteur("date_fin"))
      .drop(df_latestProducteur("date_modif"))
      .drop(df_latestProducteur("date_debut"))
      .drop(df_latestProducteur("year"))
      .drop(df_latestProducteur("month"))
      .drop(df_latestProducteur("day"))
      .drop(df_latestProducteur("id_producteur"))
      .drop(df_latestProducteur("code_producteur"))
      .drop(df_latestProducteur("id_rva"))
      .drop(df_latestProducteur("commune"))
      .drop(df_latestProducteur("code_insee"))
      .drop(df_latestProducteur("type_producteur"))
      .drop(df_latestProducteur("activite"))
      .drop(df_latestProducteur("latitude"))
      .drop(df_latestProducteur("longitude"))
      .drop(df_partitionedIncomingProducteur("id_producteur"))
      .withColumn("id_producteur",Utils.idProducteurUDF()(col("code_producteur"),lit(datePhoto)))
      .withColumn("date_modif", lit(now).cast(TimestampType))

    log("Producteurs reactives")
    df_reactivated.show(false)*/

    val dfJoined = df_updated.unionByName(df_latestProducteur.filter(col("date_fin").isNotNull))//.unionByName(df_toEnd)
    dfJoined
  }

  /**
   * Fonction qui permet de creer un dataframe avec les donnee
   *
   * @param spark : la session spark
   * @param df    : le dataframe a traiter
   * @return
   */
  def ExecuteDechetAnalysis_Producteur(spark: SparkSession, df_raw: DataFrame, SYSDATE: String, datePhoto: String,
                                       df_latestProducteur: DataFrame): DataFrame = {
    var df_preparedIncomingProducteur : DataFrame = null
    if(!df_raw.head(1).isEmpty){
      df_preparedIncomingProducteur = prepareIncomingProducteurDf(df_raw, spark)
    } else {
      df_preparedIncomingProducteur = createEmptyProducteurDf(spark)
    }
    show(df_preparedIncomingProducteur,"Producteur a traiter")

    // Ajout des informations de partitionnement
    val df_partitionedIncomingProducteur = Utils.dfToPartitionedDf(df_preparedIncomingProducteur, SYSDATE)
    show(df_partitionedIncomingProducteur,"Producteurs dans le nouveau referentiel")

    // Recuperation des bacs inconnus
    val dfUnknown = df_latestProducteur.filter(col("code_producteur").isNull)
    show(dfUnknown,"Producteur inconnu")

    // Ajout de la date de fin aux bacs supprimes
    val dfDeleted = deletedProducteurTreatment(df_partitionedIncomingProducteur, datePhoto,
      df_latestProducteur)

    // Ajout des nouveaux elements dans le referentiel
    val dfCreated = createProducteurTreatment(df_partitionedIncomingProducteur, datePhoto,
      df_latestProducteur)

    val latestActive = df_latestProducteur.filter(col("date_fin").isNull)

    val df_duplicated =  latestActive.unionByName(df_partitionedIncomingProducteur)
      .groupBy("code_producteur","id_rva","commune","code_insee","type_producteur","activite","latitude","longitude")
      .count().filter("count > 1")

    // Producteurs non changes
    val df_notChanged = latestActive.join(df_duplicated.filter(col("code_producteur").isNotNull),Seq("code_producteur"),"left_semi")
      //latestActive.filter(col("code_producteur").isInCollection(
      //df_duplicated.select("code_producteur").filter(col("code_producteur").isNotNull).rdd.map(row => row(0)).collect().toList))


    show(df_notChanged,"Producteur inchanges")

    val df_notduplicated = latestActive.unionByName(df_partitionedIncomingProducteur)
      .groupBy("code_producteur","id_rva","commune","code_insee","type_producteur","activite","latitude","longitude")
      .count().filter("count = 1")

    val df_changed = latestActive.join(df_notduplicated.filter(col("code_producteur").isNotNull),Seq("code_producteur"),"left_semi")
      //latestActive.filter(col("code_producteur").isInCollection(
      //df_notduplicated.select("code_producteur").filter(col("code_producteur").isNotNull).rdd.map(row => row(0)).collect().toList))

 
    show(df_changed,"Producteur changes")
    

    // Modification des elements existants
    val dfUpdated = updatedProducteurTreatment(df_changed, df_partitionedIncomingProducteur, datePhoto)

    val dfOld = df_latestProducteur.filter(col("date_fin").isNotNull)
    // Creation du nouveau dataframe referentiel
    val newRefential = dfDeleted.unionByName(dfCreated).unionByName(dfUpdated).unionByName(dfUnknown).unionByName(dfOld).unionByName(df_notChanged)

  
    show(newRefential,"Nouveau referentiel Producteur")

    newRefential
  }

  /**
   * Fonction qui permet de creer un dataframe avec les donnee
   *
   * @param spark : la session spark
   * @param df    : le dataframe a traiter
   * @return
   */
  def ExecuteDechetAnalysis_Bac(spark: SparkSession, df_raw: DataFrame, SYSDATE: String, datePhoto: String,
                                df_latestRecipient : DataFrame, df_latestProducteur: DataFrame): DataFrame = {
    var df_preparedIncomingRecipient : DataFrame = null
    if(!df_raw.head(1).isEmpty){
      df_preparedIncomingRecipient = prepareIncomingRecipientDf(df_raw, spark)
    } else {
      df_preparedIncomingRecipient = createEmptyBacDf(spark)
    }
    show(df_preparedIncomingRecipient,"Bac a traiter")
    // Ajout des informations de partitionnement
    var df_partitionedIncomingRecipient = Utils.dfToPartitionedDf(df_preparedIncomingRecipient, SYSDATE)

    show(df_partitionedIncomingRecipient,"Bacs dans le nouveau referentiel")
    // re attribution des id_producteur si le code_proucteur n'a pas changé
    var df_id_latest = df_latestRecipient.select("code_producteur","id_producteur").withColumnRenamed("id_producteur","id_producteur_latest")
    var df_id_incoming = df_partitionedIncomingRecipient.select("code_producteur","id_producteur").withColumnRenamed("id_producteur","id_producteur_incoming")
    val df_id_join = df_id_incoming.repartition(col("code_producteur")).join(df_id_latest.repartition(col("code_producteur")),Seq("code_producteur"),"left")
        .withColumn("id_producteur_temp", when(col("id_producteur_latest").isNull,df_id_incoming("id_producteur_incoming") )
        .otherwise(df_id_latest("id_producteur_latest")))
        .select("code_producteur", "id_producteur_temp")
        .withColumnRenamed("id_producteur_temp","id_producteur")
        .dropDuplicates()
    df_partitionedIncomingRecipient = df_partitionedIncomingRecipient.drop("id_producteur").join(df_id_join,Seq("code_producteur"), "inner")
    show(df_partitionedIncomingRecipient,"df_partitionedIncomingRecipient")

    // Recuperation des bacs inconnus
    val dfUnknown = df_latestRecipient.filter(col("code_puce") === "null"
                                              || col("code_puce") === "INCONNU")

    // Ajout de la date de fin aux bacs supprimes
    val dfDeleted = deletedRecipientTreatment(df_partitionedIncomingRecipient, datePhoto,
      df_latestRecipient)

    // Ajout des nouveaux elements dans le referentiel
    val dfCreated = createRecipientTreatment(df_partitionedIncomingRecipient, datePhoto,
      df_latestRecipient)

    val latestActive = df_latestRecipient.filter(col("date_fin").isNull)

    val df_duplicated =  latestActive.drop("id_producteur").unionByName(df_partitionedIncomingRecipient.drop("id_producteur"))
      .groupBy("code_puce","code_producteur","categorie_recipient","type_recipient","litrage_recipient","type_puce","nb_collecte")
      .count().filter("count > 1")

    show(df_duplicated,"df_duplicated")
    // Recipient non changes
    val df_notChanged = latestActive.join(df_duplicated.filter(col("code_puce").isNotNull),Seq("code_puce"),"left_semi")
      //latestActive.filter(col("code_puce").isInCollection(
      //df_duplicated.select("code_puce").filter(col("code_puce").isNotNull).rdd.map(row => row(0)).collect().toList))


    show(df_notChanged,"Bacs inchanges")

    val df_notduplicated =  latestActive.drop("id_producteur").unionByName(df_partitionedIncomingRecipient.drop("id_producteur"))
      .groupBy("code_puce","code_producteur","categorie_recipient","type_recipient","litrage_recipient","type_puce","nb_collecte")
      .count().filter("count = 1")
    show(df_notduplicated,"df_notduplicated")

    val df_changed = latestActive.join(df_notduplicated.filter(col("code_puce").isNotNull),Seq("code_puce"),"left_semi")
      //latestActive.filter(col("code_puce").isInCollection(
      //df_notduplicated.select("code_puce").filter(col("code_puce").isNotNull && col("code_puce") =!= "INCONNU" && col("code_puce") =!= "null").rdd.map(row => row(0)).collect().toList))

    show(df_changed,"Bacs changes")

    // Modification des elements existants
    val dfUpdated = updatedRecipientTreatment(df_changed, df_partitionedIncomingRecipient, datePhoto)

    val dfOld = df_latestRecipient.filter(col("date_fin").isNotNull)

    // Creation du nouveau dataframe referentiel
    val newRefential = dfDeleted.unionByName(dfCreated).unionByName(dfUpdated).unionByName(dfUnknown).unionByName(dfOld).unionByName(df_notChanged)

    show(newRefential,"Nouveau referentiel Bac")

    // detection des producteurs inconnu dans le referentiel des produteurs
    // affectation de l'id producteur pour le code producteur null
    val newRefentialWithUnknownProducteur = DechetAnalysis.joinLatestBacAndLatestProducteur(newRefential, df_latestProducteur)
      .withColumn("id_producteur", when(col("id_producteur").isNull,
        df_latestProducteur.filter(df_latestProducteur("code_producteur").isNull).first().getAs("id_producteur"))
        .otherwise(col("id_producteur")))

    show(newRefentialWithUnknownProducteur,"Nouveau referentiel Bac après traitement des producteurs inconnus")

    newRefentialWithUnknownProducteur
  }


  /**
   * Detection des nouveaux elements pour les ajouter au referentiel
   * @param df_preparedIncomingRecipient
   * @param datePhoto
   * @param newRefential
   * @return
   */
  def createRecipientTreatment(df_preparedIncomingRecipient: DataFrame, datePhoto: String, df_latest: DataFrame): DataFrame = {
    // recherche des nouveaux elements dans le referentiel
    val df_ToCreate = df_preparedIncomingRecipient.join(df_latest.filter(col("date_fin").isNull),
                      df_latest("code_puce") === df_preparedIncomingRecipient("code_puce"), "left_anti")
                      .withColumn("date_modif", lit(null).cast(TimestampType))

    show(df_ToCreate,"Nouveaux bacs")

    df_ToCreate
  }

  /**
   * Etape de mise a jour des bacs supprimes du referentiel
   * @param df_preparedIncomingRecipient
   * @param datePhoto
   * @param df_lastestRecipient
   * @return
   */
  def deletedRecipientTreatment(df_preparedIncomingRecipient : DataFrame, datePhoto: String,
                                df_latest : DataFrame) : DataFrame = {
    /*log("count df_preparedIncomingRecipient :" + df_preparedIncomingRecipient.count())
    show(df_preparedIncomingRecipient,"Bac df_preparedIncomingRecipient")
    log("count df_latest :" + df_latest.count())
    show(df_latest,"Bac df_latest")*/
    val dfDeleted = df_latest.join(df_preparedIncomingRecipient.repartition(1000,col("code_puce")),
                  df_latest("code_puce") === df_preparedIncomingRecipient("code_puce"), "left_anti")
    show(dfDeleted,"Bac dfDeleted")
    val dfDeleted2 = dfDeleted.repartition(col("code_puce")).filter(not(col("code_puce").isNull || col("code_puce") === "INCONNU" || col("code_puce") === "null")
                  && col("date_fin").isNull )
    show(dfDeleted2,"Bac dfDeleted2")
    val dfDeleted3 = dfDeleted2.withColumn("date_fin",
                              Utils.timestampWithZoneUdf()(lit(datePhoto),lit("000000")).cast(TimestampType))
                  .withColumn("date_modif",
                    lit(now).cast(TimestampType))
    show(dfDeleted3,"Bac supprimes")

    dfDeleted3
  }

  /**
   * Prepare le dataframe entrant, renommage , ajout et cast de colonnes
   * @param df_raw
   * @param spark
   * @return
   */
  def prepareIncomingRecipientDf(df_raw :DataFrame, spark: SparkSession): DataFrame = {
    try{
      val df = df_raw
        .withColumn("id_bac",Utils.idBacUdf()(col("code_puce"),col("date_photo")))
        .withColumn("code_puce",col("code_puce"))
        .withColumn("code_producteur",col("code_producteur"))
        .withColumn("categorie_recipient",col("categorie_recipient"))
        .withColumn("type_recipient",col("type_recipient"))
        .withColumn("litrage_recipient",col("litrage_recipient"))
        .withColumn("type_puce",Utils.typePuceUDF()(col("code_puce")))
        .withColumn("nb_collecte",Utils.typedeFreqUdf()(col("type_recipient"),col("frequence_om"),col("frequence_cs")))
        .withColumn("id_producteur", Utils.idProducteurUDF()(col("code_producteur"),col("date_photo")))
        .withColumn("date_debut", Utils.timestampWithZoneUdf()(col("date_photo"),lit("000000")).cast(TimestampType))
        .withColumn("date_fin",lit(null).cast(TimestampType))
        .withColumn("date_crea", lit(now).cast(TimestampType))
        .withColumn("date_modif",lit(null).cast(TimestampType))
      df.select("id_bac","code_puce","code_producteur","categorie_recipient","type_recipient","litrage_recipient","type_puce","nb_collecte","id_producteur","date_debut","date_fin","date_crea","date_modif")

    }catch{
      case e:Throwable =>
        log("ERROR durant l'enrichissement ExecuteDechetAnalysis()\n" +e)
        createEmptyBacDf(spark)
    }
  }

  /**
   * Cree un dataframe vide avec le bon schema pour les bacs
   * @param spark
   * @return
   */
  def createEmptyBacDf(spark : SparkSession): DataFrame ={
    val schema = StructType(
      List(
        StructField("id_bac", IntegerType, false),
        StructField("code_puce", StringType, false),
        StructField("code_producteur", IntegerType, false),
        StructField("categorie_recipient", StringType, false),
        StructField("type_recipient", StringType, false),
        StructField("litrage_recipient", IntegerType, false),
        StructField("type_puce", StringType, true),
        StructField("nb_collecte", FloatType, false),
        StructField("date_debut", TimestampType, false),
        StructField("date_fin", TimestampType, true),
        StructField("date_crea", TimestampType, false),
        StructField("date_modif", TimestampType, true),
        StructField("year", StringType, false), 
        StructField("month", StringType, false), 
        StructField("day", StringType, false),
        StructField("id_producteur", IntegerType, false)
      )
    )
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
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

  def selectEntryToUpdateDeleted(newDataFrame: DataFrame, lastestDataFrame: DataFrame, idCol :String) : DataFrame = {
    val selectedDataFrame = lastestDataFrame.join(newDataFrame,
      lastestDataFrame.col(idCol).equalTo(newDataFrame.col(idCol)), "left_anti")
    selectedDataFrame
  }

  def selectEntryToAddToReferential(newDataFrame: DataFrame, lastestDataFrame: DataFrame, idCol :String) : DataFrame = {
    val selectedDataFrame = newDataFrame.join(lastestDataFrame,
      lastestDataFrame.col(idCol).equalTo(newDataFrame.col(idCol)), "left_anti")
    selectedDataFrame
  }

  /**
   * Methode de jointure des bacs et des producteurs pour detecter les bacs relies a des producteurs inconnus
   * @param df_lastestBac
   * @param df_lastestProducteur
   * @return
   */
  def joinLatestBacAndLatestProducteur(df_lastestBac: DataFrame, df_latestProducteur: DataFrame): DataFrame = {
    val df_lastestBacFiltered = df_latestProducteur.select("id_producteur")
    val dfJoined = (df_lastestBac).join(df_lastestBacFiltered, df_lastestBac("id_producteur")
        === df_lastestBacFiltered("id_producteur"), "left")
      .drop(df_lastestBac("id_producteur"))

    dfJoined
  }

  def updatedRecipientTreatment(df_lastestBac: DataFrame, dfIncoming: DataFrame, datePhoto: String): DataFrame = {
    show(df_lastestBac,"df_lastestBac")
    show(dfIncoming,"dfIncoming")
    var df_updated = broadcast(df_lastestBac).filter(col("date_fin").isNull).join(dfIncoming, df_lastestBac("code_puce")
      === dfIncoming("code_puce"), "inner")
      .drop(dfIncoming("date_crea"))
      .drop(dfIncoming("date_fin"))
      .drop(dfIncoming("date_modif"))
      .withColumn("id_bac_modif", when(df_lastestBac("code_producteur") === dfIncoming("code_producteur") && df_lastestBac("code_puce") === dfIncoming("code_puce"),
        df_lastestBac("id_bac")).otherwise(dfIncoming("id_bac")))

      .withColumn("date_crea_modif", when(df_lastestBac("code_producteur") === dfIncoming("code_producteur"),
        df_lastestBac("date_crea")).otherwise(lit(now).cast(TimestampType)))
         
      .drop(df_lastestBac("code_puce"))
      .drop(df_lastestBac("categorie_recipient"))
      .drop(df_lastestBac("type_recipient"))
      .drop(df_lastestBac("litrage_recipient"))
      .drop(df_lastestBac("type_puce"))
      .drop(df_lastestBac("nb_collecte"))
      .drop(df_lastestBac("date_crea"))
      //.drop(df_lastestBac("date_modif"))
      .withColumn("id_producteur_modif", when(df_lastestBac("code_producteur") === dfIncoming("code_producteur"),
        df_lastestBac("id_producteur")).otherwise(dfIncoming("id_producteur")))
      .withColumn("date_debut_modif", when(df_lastestBac("code_producteur") === dfIncoming("code_producteur"),
        df_lastestBac("date_debut")).otherwise(dfIncoming("date_debut")))

      .withColumn("year_modif", when(df_lastestBac("code_producteur") === dfIncoming("code_producteur"),
        df_lastestBac("year")).otherwise(dfIncoming("year")))
      .withColumn("month_modif", when(df_lastestBac("code_producteur") === dfIncoming("code_producteur"),
        df_lastestBac("month")).otherwise(dfIncoming("month")))
      .withColumn("day_modif", when(df_lastestBac("code_producteur") === dfIncoming("code_producteur"),
        df_lastestBac("day")).otherwise(dfIncoming("day")))
      .drop(df_lastestBac("id_producteur"))
      .drop(df_lastestBac("code_producteur"))
      .drop(df_lastestBac("id_bac"))
      .drop(dfIncoming("id_bac"))
      .drop(dfIncoming("year"))
      .drop(dfIncoming("month"))
      .drop(dfIncoming("day"))
      .drop(df_lastestBac("year"))
      .drop(df_lastestBac("month"))
      .drop(df_lastestBac("day"))
      .drop(dfIncoming("id_producteur"))
      .drop(dfIncoming("date_debut"))
      .drop(df_lastestBac("date_debut"))
      .withColumnRenamed("id_producteur_modif","id_producteur")
      .withColumnRenamed("id_bac_modif","id_bac")
      .withColumnRenamed("date_debut_modif","date_debut")
      .withColumnRenamed("year_modif","year")
      .withColumnRenamed("month_modif","month")
      .withColumnRenamed("day_modif","day")
      .withColumnRenamed("date_crea_modif","date_crea")

    show(df_updated,"df_updated jointure")
    var df_updated_changed = df_updated.filter(col("id_bac").isInCollection(
    df_lastestBac.select("id_bac").collect.map(f=>f.getLong(0)).toList))
    show(df_updated_changed,"df_updated_changed")
    var df_crea = df_updated.filter(!col("id_bac").isInCollection(
    df_lastestBac.select("id_bac").collect.map(f=>f.getLong(0)).toList))
    show(df_crea,"df_crea")
    val dfToEnd = df_lastestBac.filter(col("date_fin").isNull)
                    .join(dfIncoming, (df_lastestBac("code_puce") === dfIncoming("code_puce") && df_lastestBac("code_producteur") =!= dfIncoming("code_producteur")),
                    "inner")
      .select(df_lastestBac("id_bac"),df_lastestBac("code_puce"));

    show(dfToEnd,"Bacs a terminer (changement de code_producteur)");

    df_updated_changed = df_updated_changed.unionByName(df_lastestBac.filter(col("id_bac").isInCollection(
      dfToEnd.select("id_bac").collect.map(f=>f.getLong(0)).toList))
      .withColumn("date_fin", Utils.timestampWithZoneUdf()(lit(datePhoto),lit("000000")).cast(TimestampType)))
      .withColumn("date_modif",lit(now).cast(TimestampType))
    show(df_updated_changed,"après avoir ajouter date_fin")
    df_updated_changed = df_updated_changed.unionByName(df_crea.withColumn("date_modif",lit(null).cast(TimestampType))
    )
    show(df_updated_changed,"Bacs mis a jour (terminer)")

   /* val df_reactivated = df_lastestBac.filter(col("date_fin").isNotNull).join(dfIncoming, df_lastestBac("code_puce")
      === dfIncoming("code_puce"), "inner")
      .drop(df_lastestBac("date_crea"))
      .drop(df_lastestBac("date_modif"))
      .drop(df_lastestBac("date_debut"))
      .drop(df_lastestBac("year"))
      .drop(df_lastestBac("month"))
      .drop(df_lastestBac("day"))
      .drop(df_lastestBac("id_bac"))
      .drop(df_lastestBac("code_puce"))
      .drop(df_lastestBac("code_producteur"))
      .drop(df_lastestBac("categorie_recipient"))
      .drop(df_lastestBac("type_recipient"))
      .drop(df_lastestBac("litrage_recipient"))
      .drop(df_lastestBac("type_puce"))
      .drop(df_lastestBac("nb_collecte"))
      .drop(df_lastestBac("id_producteur"))
      .drop(dfIncoming("id_bac"))
      .drop(dfIncoming("date_fin"))
      .withColumn("id_bac",Utils.idBacUdf()(col("code_puce"),lit(datePhoto)))
      .withColumn("date_modif", lit(now).cast(TimestampType))

    log("Bacs reactives")
    df_reactivated.show(false)*/

    val dfJoined = df_updated_changed // .unionByName(df_reactivated)

    dfJoined
  }

}