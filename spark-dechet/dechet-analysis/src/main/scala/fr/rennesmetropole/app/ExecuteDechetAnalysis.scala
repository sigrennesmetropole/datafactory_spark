package fr.rennesmetropole.app

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.services.{DechetAnalysis, ImportDechet}
import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExecuteDechetAnalysis {
  def main(args: Array[String]): Either[Unit, DataFrame] = {
    val logger = Logger(getClass.getName)
    /** SYSDATE recupère la date actuelle de l'horloge système dans le fuseau horaire par défaut (UTC) */
    var SYSDATE = java.time.LocalDate.now.toString
    try {
      SYSDATE = args(0)
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
      val df_ImportDechet = ImportDechet.ExecuteImportDechet(spark, SYSDATE, nameEnv)
      val df_lastestBac = ImportDechet.readLastestReferential(spark, SYSDATE, nameEnvRecip)
      println("IMPORT DECHET")
      df_ImportDechet.show(false)
      var df_PartitionedDechet = df_ImportDechet
      if(!df_ImportDechet.head(1).isEmpty){
        val df_AnalysedDechet = DechetAnalysis.ExecuteDechetAnalysis_Collecte(spark, df_ImportDechet,SYSDATE, df_lastestBac)
        println("ANALYSED DECHET")
        df_AnalysedDechet.show(false)

        df_PartitionedDechet = Utils.dfToPartitionedDf(df_AnalysedDechet, SYSDATE)
        println("PARTITIONED DECHET")
        df_PartitionedDechet.show(false)
      }else{
        println("Aucune donnee importe, skip du traitement...")
      }

      if (Utils.envVar("TEST_MODE") == "False" && (!df_PartitionedDechet.head(1).isEmpty)) {
        Left(Utils.writeToS3(spark,df_PartitionedDechet,nameEnv,SYSDATE))
        println("Write to s3 Done")
        Left(Utils.postgresPersist(spark, Utils.envVar("POSTGRES_URL"), df_PartitionedDechet, Utils.envVar("POSTGRES_TABLE_DECHET_NAME"),SYSDATE))
        
      }
      else {
        Right(df_PartitionedDechet)
      }
    } catch {
      case e: Throwable => {
        logger.error("Echec du traitement d'analyse Dechet")
        throw new Exception("Echec du traitement d'analyse Dechet", e)
      }
    }
  }
}
