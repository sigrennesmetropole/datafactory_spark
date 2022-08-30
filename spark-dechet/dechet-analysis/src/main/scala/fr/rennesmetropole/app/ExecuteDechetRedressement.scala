package fr.rennesmetropole.app

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.services.{DechetAnalysis, ImportDechet}
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.show
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant

object ExecuteDechetRedressement {
  def main(args: Array[String]): Either[Unit, DataFrame] = {
    val logger = Logger(getClass.getName)
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
        println("Des arguments manquent")
        println("Commande lancée :")
        println("spark-submit --class fr.rennesmetropole.app.ExecuteDechetAnalysis /app-dechet/target/rm-dechet-analysis-1.0-SNAPSHOT.jar <DATE>")
        println("DATE : 2021-05-07 => yyyy/mm/dd")
        throw new Exception("Pas d'arguments", e )
      }
    }

    /** Initialisation de la session spark */
    val spark: SparkSession = SparkSession.builder()
      .appName("Dechet Analysis")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

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

    val nameEnv = "tableCollecte"
    val nameEnvRecip = "tableRecipient"
    try {
      //Import des referentiels Bacs
      val df_lastestBac = ImportDechet.readLastestReferential(spark, SYSDATE, nameEnvRecip)
      //Import des données a redresser
      val (df_support, df_a_redresser,dateRef)= ImportDechet.importRedressement(spark,SYSDATE,nameEnv)
      show(df_support,"df_support")
      show(df_a_redresser,"df_a_redresser")
      //Redressement des données
      val df_redresse = DechetAnalysis.redressement_donne_incorrect(df_a_redresser,df_support,df_lastestBac,spark,dateRef,SYSDATE)
      Right(df_redresse)
    } catch {
      case e: Throwable => {
        logger.error("Echec du traitement de redresseement des Dechets")
        throw new Exception("Echec du traitement redresseement Dechets", e)
      }
    }
  }
}
