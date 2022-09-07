package fr.rennesmetropole.app

import fr.rennesmetropole.services.{DechetAnalysis, ImportDechet}
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object ExecuteDechetRefSmartToPostgres {
  def main(args: Array[String]): Either[Unit, DataFrame] = {
    /** SYSDATE recupère la date actuelle de l'horloge système dans le fuseau horaire par défaut (UTC) */
    var SYSDATE = java.time.LocalDate.now.toString
    try {
      SYSDATE = args(0)
    } catch {
      case e: Throwable => {
        println("Des arguments manquent")
        println("Commande lancée :")
        println("spark-submit --class fr.rennesmetropole.app.ExecuteDechetRefAnalysis /app-dechet/target/rm-dechet-analysis-1.0-SNAPSHOT.jar <DATE>")
        println("DATE : 2021-05-07 => yyyy/mm/dd")
        throw new Exception("Pas d'arguments", e )
      }
    }

    /** Initialisation de la session spark */
    val spark: SparkSession = SparkSession.builder()
      .appName("Dechet Ref Analysis")
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

    val nameEnvProd = "tableProducteur"
    val nameEnvRecip = "tableRecipient"
    try {

      val df_lastestProducteur = ImportDechet.readDatedReferential(spark, SYSDATE, nameEnvProd)
      val df_lastestBac = ImportDechet.readDatedReferential(spark, SYSDATE, nameEnvRecip)

      
      if (Utils.envVar("TEST_MODE") == "False"){
        if(!df_lastestProducteur.head(1).isEmpty && !df_lastestBac.head(1).isEmpty){
          Left(Utils.postgresPersistOverwrite(spark, Utils.envVar("POSTGRES_URL"), df_lastestProducteur, 
             Utils.envVar("POSTGRES_TABLE_DECHET_PRODUCTEUR")))
          Left(Utils.postgresPersistOverwrite(spark, Utils.envVar("POSTGRES_URL"), df_lastestBac, 
             Utils.envVar("POSTGRES_TABLE_DECHET_BAC")))
        } else {
          logger.error("Données non enregistrée car un des référentiels est vide")
          Right(df_lastestProducteur)
        }

      }
      else {
        Right(df_lastestProducteur)
        Right(df_lastestBac)
      }
    } catch {
      case e: Throwable => {
        logger.error("Echec du traitement d'analyse Dechet")
        throw new Exception("Echec du traitement d'analyse Dechet", e)
      }
    }
  }
}
