package fr.rennesmetropole.app

import fr.rennesmetropole.services.{DechetAnalysis, ImportDechet}
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.{logger, show}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant
import fr.rennesmetropole.tools.Utils.log
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.{col, date_format}
object ExecuteDechetRedressement {
  def main(args: Array[String]): Either[Unit, DataFrame] = {
    /** SYSDATE recupère la date actuelle de l'horloge système dans le fuseau horaire par défaut (UTC) */
    var SYSDATE = java.time.LocalDate.now.toString
    try {
      SYSDATE = args(0)
      if(Utils.envVar("TEST_MODE") != "False" && args.length>1){
        if(args(1).nonEmpty) {
          DechetAnalysis.setNow(Instant.parse(args(1)))
        }
      }
    } catch {
      case e: Throwable => {
        log("Des arguments manquent")
        log("Commande lancée :")
        log("spark-submit --class fr.rennesmetropole.app.ExecuteDechetAnalysis /app-dechet/target/rm-dechet-analysis-1.0-SNAPSHOT.jar <DATE>")
        log("DATE : 2021-05-07 => yyyy/mm/dd")
        throw new Exception("Pas d'arguments", e )
      }
    }

    /** Initialisation de la session spark */
    val spark: SparkSession = SparkSession.builder()
      .appName("Dechet Analysis")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.sql.broadcastTimeout", "600")
      .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
      .getOrCreate()
    import org.apache.sedona.sql.utils.SedonaSQLRegistrator
    SedonaSQLRegistrator.registerAll(spark)
    SedonaVizRegistrator.registerAll(spark)
    /** Chargement des paramètres Hadoop à partir des propriétés système */
    spark.sparkContext
      .hadoopConfiguration
      .set("fs.s3a.access.key", Utils.envVar("S3_ACCESS_KEY"))
    spark.sparkContext
      .hadoopConfiguration
      .set("fs.s3a.secret.key", Utils.envVar("S3_SECRET_KEY"))
    spark.sparkContext
      .hadoopConfiguration
      .set("fs.s3a.endpoint", Utils.envVar("S3_ENDPOINT"))
    spark.sparkContext
      .hadoopConfiguration
      .set("fs.s3a.path.style.access", "true")
    spark.sparkContext
      .hadoopConfiguration
      .set("fs.s3.aws.credentials.provider", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    spark.conf.set("spark.sql.adaptive.enabled", true)
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled",true)


    val nameEnv = "tableCollecte"
    val nameEnvRecip = "tableRecipient"
    var exception = ""
    var df_redresse_spatial = spark.emptyDataFrame
    try {
      //Import des referentiels BacsreadLastestReferential
      val df_lastestBac = ImportDechet.readLastestReferential(spark, SYSDATE, nameEnvRecip)
      //Import des données a redresser
      val (df_support, df_a_redresser,dateRef)= ImportDechet.importRedressement(spark,SYSDATE,nameEnv)
      show(df_support,"df_support")
      show(df_a_redresser,"df_a_redresser")
      if(!df_a_redresser.head(1).isEmpty){
        //Redressement des données
        log("Redressement du poids des données déchets")
        val df_redresse = DechetAnalysis.redressement_donne_incorrect(df_a_redresser, df_support, df_lastestBac, spark, dateRef, SYSDATE)

        if (df_redresse.head(1).isEmpty) {
          exception = exception + "Pas de données collecte redresse a écrire dans minio, arrêt de la chaine de traitement... \n"
        }else{
            log("Redressement des données spatial des données déchets")
            val commune_emprise = Utils.readFomPostgres(spark, Utils.envVar("POSTGRES_URL"), Utils.envVar("POSTGRES_TABLE_COMMUNE_EMPRISE"))
            val quartier = Utils.readFomPostgres(spark, Utils.envVar("POSTGRES_URL"), Utils.envVar("POSTGRES_TABLE_QUARTIER"))
            df_redresse_spatial = DechetAnalysis.redressement_donne_spatial(df_redresse, commune_emprise, quartier, spark, SYSDATE)
            if (df_redresse_spatial.head(1).isEmpty) {
              exception = exception + "Pas de données collecte spatiale a écrire dans minio, arrêt de la chaine de traitement... \n"
            }
          }
        if (exception != "" && Utils.envVar("TEST_MODE") == "False") {
          logger.error("exception :" + exception)
          throw new Exception(exception)
        }
        if (Utils.envVar("TEST_MODE") == "False") {
          Left(Utils.writeToS3(spark, df_redresse_spatial, nameEnv, SYSDATE))
        }
      }
      else{
        throw new Exception("Pas de données à redresser, erreur lors de l'import des données ou pas de donné dans minio.")
      }
      logger.error("exception :" + exception)
      if (exception != "" && Utils.envVar("TEST_MODE") == "False") {
        throw new Exception(exception)
      } else {
        Right(df_redresse_spatial)
      }


    } catch {
      case e: Throwable => {
        logger.error("Echec du traitement de redresseement des Dechets")
        throw new Exception("Echec du traitement redresseement Dechets", e)
      }
    }
  }
}
