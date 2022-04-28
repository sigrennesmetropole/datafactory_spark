package fr.rennesmetropole.services

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.sql.Timestamp

object DechetAnalysis {

  val logger = Logger(LoggerFactory.getLogger(this.getClass))
  val frTZ = java.time.ZoneId.of("Europe/Paris")
  val now = Timestamp.from(java.time.ZonedDateTime.now(frTZ).withNano(0).toInstant)
  /**
   * Fonction qui permet de créer un dataframe avec les donnée
   *
   * @param spark : la session spark
   * @param df    : le dataframe à traiter
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
    if(!df_raw.head(1).isEmpty){
      try{
        val df_lastestBacFiltered = df_lastestBac.select("id_bac", "code_puce","date_debut","date_fin")
        val dfJoined = df_raw.join(df_lastestBacFiltered,
          df_raw("Code_puce") <=> df_lastestBacFiltered("code_puce") &&
            Utils.timestampWithoutZone()(col("Date_de_la_levee"),col("Heure_de_la_levee")) >= df_lastestBacFiltered("date_debut")
            && (df_lastestBacFiltered("date_fin").isNull
            || Utils.timestampWithoutZone()(col("Date_de_la_levee"),col("Heure_de_la_levee")) <= df_lastestBacFiltered("date_fin")), "left")
          .drop(df_lastestBacFiltered("code_puce"))

         //dfJoined.show()

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

       /* df_withTimestamp = df_withTimestamp.drop("id_bac").withColumnRenamed("id_bac_by_date","id_bac")
          .filter(col("date_mesure") > col("date_fin")
            || col("date_mesure") < col("date_debut"))*/

        //df_withTimestamp.show(false)
        df_withTimestamp.select("date_mesure","code_puce", "id_bac","code_tournee","code_immat","poids","poids_corr","latitude","longitude","date_crea","date_modif")
      }catch{
        case e:Throwable =>
        println("ERROR durant l'enrichissement ExecuteDechetAnalysis()\n" +e)
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
      }
    }else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
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
        println("ERROR durant l'enrichissement ExecuteDechetAnalysis()\n" +e)
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
        StructField("date_modif", TimestampType, true)
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

    println("Producteurs supprimés")
    dfDeleted.show()

    dfDeleted
  }

  def createProducteurTreatment(df_partitionedIncomingProducteur: DataFrame, datePhoto: String, df_latest: DataFrame) = {
    val df_ToCreate = df_partitionedIncomingProducteur.join(df_latest.filter(col("date_fin").isNull),
      df_latest("code_producteur") === df_partitionedIncomingProducteur("code_producteur") , "left_anti")
      .withColumn("date_modif", lit(now).cast(TimestampType))

    println("Nouveaux producteurs")
    df_ToCreate.show()

    df_ToCreate
  }

  def updatedProducteurTreatment(df_latestProducteur: DataFrame, df_partitionedIncomingProducteur: DataFrame, datePhoto: String) = {
    var df_updated = df_latestProducteur.filter(col("date_fin").isNull).join(df_partitionedIncomingProducteur, df_latestProducteur("code_producteur")
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

    println("Producteurs mis à jour")
    df_updated.show()

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

    println("Producteurs réactivés")
    df_reactivated.show()*/

    val dfJoined = df_updated.unionByName(df_latestProducteur.filter(col("date_fin").isNotNull))//.unionByName(df_toEnd)
    dfJoined
  }

  /**
   * Fonction qui permet de créer un dataframe avec les donnée
   *
   * @param spark : la session spark
   * @param df    : le dataframe à traiter
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
    println("Producteur à traiter")
    df_preparedIncomingProducteur.show()

    // Ajout des informations de partitionnement
    val df_partitionedIncomingProducteur = Utils.dfToPartitionedDf(df_preparedIncomingProducteur, SYSDATE)
    println("Producteurs dans le nouveau référentiel")
    df_partitionedIncomingProducteur.show()


    // Récupération des bacs inconnus
    val dfUnknown = df_latestProducteur.filter(col("code_producteur").isNull)
    println("Producteur inconnu")
    dfUnknown.show()

    // Ajout de la date de fin aux bacs supprimés
    val dfDeleted = deletedProducteurTreatment(df_partitionedIncomingProducteur, datePhoto,
      df_latestProducteur)

    // Ajout des nouveaux élements dans le référentiel
    val dfCreated = createProducteurTreatment(df_partitionedIncomingProducteur, datePhoto,
      df_latestProducteur)

    val latestActive = df_latestProducteur.filter(col("date_fin").isNull)

    val df_duplicated =  latestActive.unionByName(df_partitionedIncomingProducteur)
      .groupBy("code_producteur","id_rva","commune","code_insee","type_producteur","activite","latitude","longitude")
      .count().filter("count > 1")

    // Producteurs non changés
    val df_notChanged = latestActive.filter(col("code_producteur").isInCollection(
      df_duplicated.select("code_producteur").filter(col("code_producteur").isNotNull).rdd.map(row => row(0)).collect().toList))

    println("Producteur inchangés")
    df_notChanged.show()

    val df_notduplicated = latestActive.unionByName(df_partitionedIncomingProducteur)
      .groupBy("code_producteur","id_rva","commune","code_insee","type_producteur","activite","latitude","longitude")
      .count().filter("count = 1")

    val df_changed = latestActive.filter(col("code_producteur").isInCollection(
      df_notduplicated.select("code_producteur").filter(col("code_producteur").isNotNull).rdd.map(row => row(0)).collect().toList))

    println("Producteur changés")
    df_changed.show()

    // Modification des élements existants
    val dfUpdated = updatedProducteurTreatment(df_changed, df_partitionedIncomingProducteur, datePhoto)

    val dfOld = df_latestProducteur.filter(col("date_fin").isNotNull)
    // Création du nouveau dataframe référentiel
    val newRefential = dfDeleted.unionByName(dfCreated).unionByName(dfUpdated).unionByName(dfUnknown).unionByName(dfOld).unionByName(df_notChanged)

    println("Nouveau référentiel Producteur")
    newRefential.show()

    newRefential
  }

  /**
   * Fonction qui permet de créer un dataframe avec les donnée
   *
   * @param spark : la session spark
   * @param df    : le dataframe à traiter
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
    println("Bac à traiter")
    df_preparedIncomingRecipient.show()
    // Ajout des informations de partitionnement
    val df_partitionedIncomingRecipient = Utils.dfToPartitionedDf(df_preparedIncomingRecipient, SYSDATE)

    println("Bacs dans le nouveau référentiel")
    df_partitionedIncomingRecipient.show()

    // Récupération des bacs inconnus
    val dfUnknown = df_latestRecipient.filter(col("code_puce").isNull
                                              || col("code_puce") === "INCONNU")

    // Ajout de la date de fin aux bacs supprimés
    val dfDeleted = deletedRecipientTreatment(df_partitionedIncomingRecipient, datePhoto,
      df_latestRecipient)

    // Ajout des nouveaux élements dans le référentiel
    val dfCreated = createRecipientTreatment(df_partitionedIncomingRecipient, datePhoto,
      df_latestRecipient)

    val latestActive = df_latestRecipient.filter(col("date_fin").isNull)

    val df_duplicated =  latestActive.unionByName(df_partitionedIncomingRecipient)
      .groupBy("code_puce","code_producteur","categorie_recipient","type_recipient","litrage_recipient","type_puce","nb_collecte","id_producteur")
      .count().filter("count > 1")

    // Producteurs non changés
    val df_notChanged = latestActive.filter(col("code_puce").isInCollection(
      df_duplicated.select("code_puce").filter(col("code_puce").isNotNull).rdd.map(row => row(0)).collect().toList))

    println("Bacs inchangés")
    df_notChanged.show()

    val df_notduplicated =  latestActive.unionByName(df_partitionedIncomingRecipient)
      .groupBy("code_puce","code_producteur","categorie_recipient","type_recipient","litrage_recipient","type_puce","nb_collecte","id_producteur")
      .count().filter("count = 1")

    val df_changed = latestActive.filter(col("code_puce").isInCollection(
      df_notduplicated.select("code_puce").filter(col("code_puce").isNotNull).rdd.map(row => row(0)).collect().toList))

    println("Bacs changés")
    df_changed.show()

    // Modification des élements existants
    val dfUpdated = updatedRecipientTreatment(df_changed, df_partitionedIncomingRecipient, datePhoto)

    val dfOld = df_latestRecipient.filter(col("date_fin").isNotNull)

    // Création du nouveau dataframe référentiel
    val newRefential = dfDeleted.unionByName(dfCreated).unionByName(dfUpdated).unionByName(dfUnknown).unionByName(dfOld).unionByName(df_notChanged)

    println("Nouveau référentiel Bac")
    newRefential.show()

    // détection des producteurs inconnu dans le référentiel des produteurs
    // affectation de l'id producteur pour le code producteur null
    val newRefentialWithUnknownProducteur = DechetAnalysis.joinLatestBacAndLatestProducteur(newRefential, df_latestProducteur)
      .withColumn("id_producteur", when(col("id_producteur").isNull,
        df_latestProducteur.filter(df_latestProducteur("code_producteur").isNull).first().getAs("id_producteur"))
        .otherwise(col("id_producteur")))

    println("Nouveau référentiel Bac après traitement des producteurs inconnus")
    newRefentialWithUnknownProducteur.show()

    newRefentialWithUnknownProducteur
  }


  /**
   * Détection des nouveaux éléments pour les ajouter au référentiel
   * @param df_preparedIncomingRecipient
   * @param datePhoto
   * @param newRefential
   * @return
   */
  def createRecipientTreatment(df_preparedIncomingRecipient: DataFrame, datePhoto: String, df_latest: DataFrame): DataFrame = {
    // recherche des nouveaux éléments dans le référentiel
    val df_ToCreate = df_preparedIncomingRecipient.join(df_latest.filter(col("date_fin").isNull),
                      df_latest("code_puce") === df_preparedIncomingRecipient("code_puce"), "left_anti")
                      .withColumn("date_modif", lit(null).cast(TimestampType))

    println("Nouveaux bacs")
    df_ToCreate.show()

    df_ToCreate
  }

  /**
   * Etape de mise a jour des bacs supprimés du référentiel
   * @param df_preparedIncomingRecipient
   * @param datePhoto
   * @param df_lastestRecipient
   * @return
   */
  def deletedRecipientTreatment(df_preparedIncomingRecipient : DataFrame, datePhoto: String,
                                df_latest : DataFrame) : DataFrame = {
    val dfDeleted = df_latest.join(df_preparedIncomingRecipient,
                  df_latest("code_puce") === df_preparedIncomingRecipient("code_puce"), "left_anti")
                  .filter(not(col("code_puce").isNull || col("code_puce") === "INCONNU" )
                  && col("date_fin").isNull )
                  .withColumn("date_fin",
                              Utils.timestampWithZoneUdf()(lit(datePhoto),lit("000000")).cast(TimestampType))
                  .withColumn("date_modif",
                    lit(now).cast(TimestampType))
    println("Bac supprimés")
    dfDeleted.show()

    dfDeleted
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
        .withColumn("date_modif",lit(null).cast(TimestampType).cast(TimestampType))
      df.select("id_bac","code_puce","code_producteur","categorie_recipient","type_recipient","litrage_recipient","type_puce","nb_collecte","id_producteur","date_debut","date_fin","date_crea","date_modif")

    }catch{
      case e:Throwable =>
        println("ERROR durant l'enrichissement ExecuteDechetAnalysis()\n" +e)
        createEmptyBacDf(spark)
    }
  }

  /**
   * Créé un dataframe vide avec le bon schema pour les bacs
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
        StructField("id_producteur", IntegerType, false),
        StructField("date_debut", TimestampType, false),
        StructField("date_fin", TimestampType, true),
        StructField("date_crea", TimestampType, false),
        StructField("date_modif", TimestampType, true)
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
   * Methode de jointure des bacs et des producteurs pour detecter les bacs reliés à des producteurs inconnus
   * @param df_lastestBac
   * @param df_lastestProducteur
   * @return
   */
  def joinLatestBacAndLatestProducteur(df_lastestBac: DataFrame, df_latestProducteur: DataFrame): DataFrame = {
    val df_lastestBacFiltered = df_latestProducteur.select("id_producteur")
    val dfJoined = df_lastestBac.join(df_lastestBacFiltered, df_lastestBac("id_producteur")
        === df_lastestBacFiltered("id_producteur"), "left")
      .drop(df_lastestBac("id_producteur"))

    dfJoined
  }

  def updatedRecipientTreatment(df_lastestBac: DataFrame, dfIncoming: DataFrame, datePhoto: String): DataFrame = {
    var df_updated = df_lastestBac.filter(col("date_fin").isNull).join(dfIncoming, df_lastestBac("code_puce")
      === dfIncoming("code_puce"), "inner")
      .drop(dfIncoming("date_crea"))
      .drop(dfIncoming("date_fin"))
      .drop(dfIncoming("date_modif"))
      .withColumn("id_bac_modif", when(df_lastestBac("code_producteur") === dfIncoming("code_producteur"),
        df_lastestBac("id_bac")).otherwise(dfIncoming("id_bac")))
      .drop(df_lastestBac("code_puce"))
      .drop(df_lastestBac("categorie_recipient"))
      .drop(df_lastestBac("type_recipient"))
      .drop(df_lastestBac("litrage_recipient"))
      .drop(df_lastestBac("type_puce"))
      .drop(df_lastestBac("nb_collecte"))
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
      .withColumn("date_modif", lit(now).cast(TimestampType))

    println("Bacs mis à jour")

val dfToEnd = df_lastestBac.filter(col("date_fin").isNull).join(dfIncoming, df_lastestBac("code_puce")
  === dfIncoming("code_puce"), "inner").filter(df_lastestBac("code_producteur") =!= dfIncoming("code_producteur"))
  .select(df_lastestBac("id_bac"),df_lastestBac("code_puce"));

dfToEnd.show();

df_updated = df_updated.unionByName(df_lastestBac.filter(col("id_bac").isInCollection(
  dfToEnd.select("id_bac").collect.map(f=>f.getLong(0)).toList))
  .withColumn("date_fin", Utils.timestampWithZoneUdf()(lit(datePhoto),lit("000000")).cast(TimestampType)))
  
df_updated.show()

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

    println("Bacs réactivés")
    df_reactivated.show()*/

    val dfJoined = df_updated// .unionByName(df_reactivated)

    dfJoined
  }

}