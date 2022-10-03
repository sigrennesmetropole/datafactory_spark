package fr.rennesmetropole.app

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.services.{DechetPreparation, ImportDechet}
import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}
object ExecuteDechetPreparation {
  def main(args: Array[String]): Either[Unit, DataFrame] = {
    val logger = Logger(getClass.getName)

    /** SYSDATE recupère la date actuelle de l'horloge système dans le fuseau horaire par défaut (UTC) */
    var SYSDATE = java.time.LocalDate.now.toString
    var csv = "false"
    try {
      SYSDATE = args(0)
      if(args.length>=2){
        csv = args(1)
      }
    } catch {
      case e: Throwable => {
        println("Des arguments manquent")
        println("Commande lancée :")
        println("spark-submit --class fr.rennesmetropole.app.ExecuteDechetPreparation /app-dechet/target/rm-dechet-preparation-1.0-SNAPSHOT.jar <DATE> <csv>")
        println("DATE : 2021-05-07 => yyyy/mm/dd")
        println("csv : csv/true => active l'écriture d'un fichier au format csv en plus du fichier ORC")
        throw new Exception("Pas d'arguments", e )
      }
    }

    /** Initialisation de la session spark */
    val spark: SparkSession = SparkSession.builder()
      .appName("Dechet Preparation")
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
    var exception = ""
    try {
      val df_ImportDechet = ImportDechet.ExecuteImportDechet(spark, SYSDATE,nameEnv)
      println("IMPORT DECHET")
      df_ImportDechet.show(false)
      var df_PreparationDechet = df_ImportDechet

    if(!df_ImportDechet.head(1).isEmpty){
      df_PreparationDechet = DechetPreparation.ExecuteDechetPreparation(spark, df_ImportDechet,nameEnv)
      println("PREPARED DECHET")
      df_PreparationDechet.show(false)
    }else {
        println("Aucune donnee importe, skip du traitement...")
      }

      if (df_PreparationDechet.head(1).isEmpty) {
        exception = exception + "Pas de données collecte préparé a écrire dans minio, arrêt de la chaine de traitement... \n"
      }
      logger.error("exception :" + exception)
      if (exception != "" && Utils.envVar("TEST_MODE") == "False") {
        throw new Exception(exception)
      }

      if (Utils.envVar("TEST_MODE") == "False" && (!df_PreparationDechet.head(1).isEmpty)) {
        Left(Utils.writeToS3(spark,df_PreparationDechet,nameEnv,csv,SYSDATE))
      }
      else {
        Right(df_PreparationDechet)
      }
    } catch {
      case e: Throwable => {
        logger.error("Echec de la preparation Dechet")
        throw new Exception("Echec de la preparation Dechet", e)
      }
    }
  }
}
