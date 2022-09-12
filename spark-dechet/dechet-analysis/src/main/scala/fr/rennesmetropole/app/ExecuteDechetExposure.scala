package fr.rennesmetropole.app

import fr.rennesmetropole.services.{DechetAnalysis, ImportDechet}
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.rennesmetropole.tools.Utils.log
object ExecuteDechetExposure {
  def main(args: Array[String]): Either[Unit, DataFrame] = {
    /** SYSDATE recupère la date actuelle de l'horloge système dans le fuseau horaire par défaut (UTC) */
    var SYSDATE = java.time.LocalDate.now.toString
    try {
      SYSDATE = args(0)
    } catch {
      case e: Throwable => {
        log("Des arguments manquent")
        log("Commande lancée :")
        log("spark-submit --class fr.rennesmetropole.app.ExecuteDechetExposure /app-dechet/target/rm-dechet-analysis-1.0-SNAPSHOT.jar <DATE>")
        log("DATE : 2021-05-07 => yyyy/mm/dd")
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
    try {
      val df_ImportDechet = ImportDechet.readSmartDataFromMinio(spark, SYSDATE, nameEnv)

      if (Utils.envVar("TEST_MODE") == "False" && (!df_ImportDechet.head(1).isEmpty)) {
        df_ImportDechet.show(false)
        Left(Utils.postgresPersist(spark,
          Utils.envVar("POSTGRES_URL"),
          df_ImportDechet,
          Utils.envVar("POSTGRES_TABLE_DECHET_NAME"),
          SYSDATE))
        Left(Utils.postgresPersist(spark,
          Utils.envVar("POSTGRES_URL"),
          df_ImportDechet,
          Utils.envVar("POSTGRES_TABLE_DECHET_API_RUDI"),
          SYSDATE))
      }
      else {
        Right(df_ImportDechet)
      }
    } catch {
      case e: Throwable => {
        logger.error("Echec du traitement d'exposition des dechets")
        throw new Exception("Echec du traitement d'exposition des dechets", e)
      }
    }
  }
}
