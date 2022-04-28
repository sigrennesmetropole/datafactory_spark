package fr.rennesmetropole.app

import com.typesafe.scalalogging.LazyLogging
import fr.rennesmetropole.services.{DechetAnalysis, ImportDechet}
import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object ExecuteDechetRefAnalysis extends LazyLogging{
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


      val df_lastestProducteur = ImportDechet.readLastestReferential(spark, SYSDATE, nameEnvProd)
      val df_lastestBac = ImportDechet.readLastestReferential(spark, SYSDATE, nameEnvRecip)

      val df_ImportDechetProducteur = ImportDechet.ExecuteImportDechet(spark, SYSDATE, nameEnvProd)
      println("IMPORT DECHET Ref Producteur")
      df_ImportDechetProducteur.show(false)

      var df_AnalysedDechetProducteur :DataFrame = null
      if(!df_ImportDechetProducteur.head(1).isEmpty){
        // Récupération de la date photo
        val datePhoto: String = df_ImportDechetProducteur.first().getAs("date_photo")

        df_AnalysedDechetProducteur = DechetAnalysis.ExecuteDechetAnalysis_Producteur(spark,
          df_ImportDechetProducteur, SYSDATE, datePhoto, df_lastestProducteur)

      }else{
        df_AnalysedDechetProducteur = df_lastestProducteur
        println("Aucune donnee importe, skip du traitement...")
      }

      // Traitement des bacs
      var df_AnalysedDechetBac :DataFrame = null

      val df_ImportDechetBac = ImportDechet.ExecuteImportDechet(spark, SYSDATE, nameEnvRecip)
      println("IMPORT DECHET Ref Bac")
      df_ImportDechetBac.show(false)

      if(!df_ImportDechetBac.head(1).isEmpty){
        // Récupération de la date photo
        val datePhoto: String = df_ImportDechetBac.first().getAs("date_photo")

        // Traitement d'enrichissement des bacs
        df_AnalysedDechetBac = DechetAnalysis.ExecuteDechetAnalysis_Bac(spark, df_ImportDechetBac,SYSDATE,
          datePhoto, df_lastestBac, df_AnalysedDechetProducteur)

      }else{
        df_AnalysedDechetBac = df_lastestBac
        println("Aucune donnee importe, skip du traitement...")
      }

      if (Utils.envVar("TEST_MODE") == "False"){
        
        if(!df_AnalysedDechetProducteur.head(1).isEmpty){
          Left(Utils.writeToS3(spark,df_AnalysedDechetProducteur,nameEnvProd,SYSDATE))
         }
        else {
          Right(df_AnalysedDechetProducteur)
        }
        
        if(!df_AnalysedDechetBac.head(1).isEmpty){
          Left(Utils.writeToS3(spark,df_AnalysedDechetBac,nameEnvRecip,SYSDATE))
        }
        else {
          Right(df_AnalysedDechetBac)
        }

        if(!df_AnalysedDechetProducteur.head(1).isEmpty && !df_AnalysedDechetBac.head(1).isEmpty){
          Left(Utils.postgresPersistOverwrite(spark, Utils.envVar("POSTGRES_URL"), df_AnalysedDechetProducteur, 
             Utils.envVar("POSTGRES_TABLE_DECHET_PRODUCTEUR")))
          Left(Utils.postgresPersistOverwrite(spark, Utils.envVar("POSTGRES_URL"), df_AnalysedDechetBac, 
             Utils.envVar("POSTGRES_TABLE_DECHET_BAC")))
        } else {
          logger.error("Données non enregistrée car un des référentiels est vide")
          Right(df_AnalysedDechetProducteur)
        }

      }
      else {
        Right(df_AnalysedDechetProducteur)
        Right(df_AnalysedDechetBac)
      }
    } catch {
      case e: Throwable => {
        logger.error("Echec du traitement d'analyse Dechet")
        throw new Exception("Echec du traitement d'analyse Dechet", e)
      }
    }
  }
}
