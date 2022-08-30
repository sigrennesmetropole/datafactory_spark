package fr.rennesmetropole.app

import com.typesafe.scalalogging.LazyLogging
import fr.rennesmetropole.services.{DechetAnalysis, ImportDechet}
import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.util.Properties

object ExecuteDechetRefPatchORC extends LazyLogging{
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
      println("Patch en cours...")
      val df_lastestBac = ImportDechet.readDatedReferential(spark, SYSDATE, nameEnvRecip).withColumn("litrage_recipient_temp",col("litrage_recipient").cast(IntegerType)).drop(col("litrage_recipient")).withColumnRenamed("litrage_recipient_temp","litrage_recipient")
      if (Utils.envVar("TEST_MODE") == "False"){
        if(!df_lastestBac.head(1).isEmpty){
          Left(Utils.writeToS3ForPatchOrc(spark,df_lastestBac,nameEnvRecip,SYSDATE))
        }
        else {
          Right(df_lastestBac)
        }
        } else {
          logger.error("Données non enregistrée car un des référentiels est vide")
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
