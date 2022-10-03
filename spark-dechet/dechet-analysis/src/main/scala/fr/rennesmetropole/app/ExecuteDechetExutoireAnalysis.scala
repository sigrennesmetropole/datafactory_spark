package fr.rennesmetropole.app

import fr.rennesmetropole.services.{DechetExutoireAnalysis, ImportDechetExutoire}
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant
import fr.rennesmetropole.tools.Utils.log
object ExecuteDechetExutoireAnalysis {
  def main(args: Array[String]): Either[Unit, DataFrame] = {
    /** SYSDATE recupère la date actuelle de l'horloge système dans le fuseau horaire par défaut (UTC) */
    var SYSDATE = java.time.LocalDate.now.toString  
    var reprise = "false"
    try {
      SYSDATE = args(0)
      if(args.length>=2) {
        reprise = args(1)
        if (Utils.envVar("TEST_MODE") != "False" && args.length > 1) {
          if (args(1).nonEmpty) {
            DechetExutoireAnalysis.setNow(Instant.parse(args(1)))
          }
        }
      }
    } catch {

      case e: Throwable => {
        log("Des arguments manquent")
        log("Commande lancée :")
        log("spark-submit --class fr.rennesmetropole.app.ExecuteDechetExutoireAnalysis /app-dechet/target/rm-dechet-analysis-1.0-SNAPSHOT.jar <DATE>")
        log("DATE : 2021-01-13 => yyyy/mm/dd")
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

    val nameEnv = "tableExutoire"
    try {
      val df_ImportDechetExutoire = ImportDechetExutoire.ExecuteImportDechetExutoire(spark, SYSDATE, nameEnv)
      log("IMPORT DECHET Donnee exutoires")
      df_ImportDechetExutoire.show(false)
      val df_AnalysedDechetExutoire = DechetExutoireAnalysis.ExecuteDechetExutoireAnalysis(spark, df_ImportDechetExutoire,SYSDATE,reprise)
      log("ANALYSED DECHET Donnee exutoires")
      df_AnalysedDechetExutoire.show(false)

      val df_PartitionedDechetExutoire = Utils.dfToPartitionedDf(df_AnalysedDechetExutoire, SYSDATE)
      log("PARTITIONED DECHET Donnees exutoires")
      df_PartitionedDechetExutoire.show(false)
      if (Utils.envVar("TEST_MODE") == "False"){
        
        if(!df_PartitionedDechetExutoire.head(1).isEmpty){
          Left(Utils.writeToS3(spark,df_PartitionedDechetExutoire,nameEnv, SYSDATE))
          //TODO job exposure pour exutoire
          Left(Utils.postgresPersist(spark, Utils.envVar("POSTGRES_URL"), df_PartitionedDechetExutoire, Utils.envVar("POSTGRES_TABLE_DECHET_EXUTOIRE"),SYSDATE))
        }
        else {
          Right(df_PartitionedDechetExutoire)
        }         
      }
      else {
        Right(df_PartitionedDechetExutoire)
      }
    } catch {
      case e: Throwable => {
        logger.error("Echec du traitement d'analyse des données exutoires")
        throw new Exception("Echec du traitement d'analyse des données exutoires", e)
      }
    }
  }
}
