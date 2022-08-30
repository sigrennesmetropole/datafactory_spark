package fr.rennesmetropole.app

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.services.{DechetExutoirePreparation, ImportDechetExutoire}
import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}


object ExecuteDechetExutoirePreparation {
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
        println("spark-submit --class fr.rennesmetropole.app.ExecuteDechetExutoirePreparation /app-dechet/target/rm-dechet-preparation-1.0-SNAPSHOT.jar <DATE> <csv>")
        println("DATE : 2022-01-13 => yyyy/mm/dd")
        println("csv : csv/true => active l'écriture d'un fichier au format csv en plus du fichier ORC")
        throw new Exception("Pas d'arguments", e )
      }
    }

    /** Initialisation de la session spark */
    val spark: SparkSession = SparkSession.builder()
      .appName("Dechet Preparation des données exutoires")
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
      val df_ImportDechetExutoire = ImportDechetExutoire.ExecuteImportDechetExutoire(spark, SYSDATE,nameEnv)
     // println("IMPORT EXUTOIRE")
     // df_ImportDechetExutoire.show(false)
     
      var df_PreparationDechetExutoire = df_ImportDechetExutoire
      if(!df_ImportDechetExutoire.head(1).isEmpty){
        df_PreparationDechetExutoire = DechetExutoirePreparation.ExecuteDechetExutoirePreparation(spark, df_ImportDechetExutoire,nameEnv)
        println("PREPARED DECHET")
        df_PreparationDechetExutoire.show(false)
      }else {
        println("TEST Aucune donnee importe, skip du traitement...")
      }
      println("Nombre de ligne a écrire : " + df_PreparationDechetExutoire.count())

      df_PreparationDechetExutoire = df_PreparationDechetExutoire.filter(col("immat").isNotNull &&
        col("date_service_vehic").isNotNull &&
        col("description_tournee").isNotNull &&
        col("km_realise").isNotNull &&
        col("no_bon").isNotNull &&
        col("lot").isNotNull &&
        col("service").isNotNull &&
        col("nom_rech_lieu_de_vidage").isNotNull &&
        col("multiples_lignes").isNotNull &&
        col("cle_unique_ligne_ticket").isNotNull
      )
      println("Nombre de ligne a écrire après suppression de ligne vide : " + df_PreparationDechetExutoire.count())

      /* val df_PreparationDechetExutoire = DechetExutoirePreparation.ExecuteDechetExutoirePreparation(spark, df_ImportDechetExutoire,nameEnv)
      println("PREPARED EXUTOIRE")
      df_PreparationDechetExutoire.show(false) */

      if (Utils.envVar("TEST_MODE") == "False" /* && (!df_PreparationDechetExutoire.head(1).isEmpty) */) {
        Left(Utils.writeToS3(spark,df_PreparationDechetExutoire,nameEnv,csv, SYSDATE))
      }
      else {
        Right(df_PreparationDechetExutoire)
      }
    } catch {
      case e: Throwable => {
        logger.error("Echec de la preparation des données exutoires Dechet")
        throw new Exception("Echec de la preparation des données exutoires Dechet", e)
      }
    }
  }
}
