package fr.rennesmetropole.app

import fr.rennesmetropole.services.ImportDechet.createEmptyBacDataFrame

import fr.rennesmetropole.services.{DechetAnalysis, ImportDechet}
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.Instant


object ExecuteDechetRefAnalysisTest{
  def main(args: Array[String]):  Unit = {
    /** SYSDATE recupère la date actuelle de l'horloge système dans le fuseau horaire par defaut (UTC) */
    println(" **** ExecuteDechetRefAnalysis ***** ")
    var SYSDATE = java.time.LocalDate.now.toString

    
    try {
      SYSDATE = args(0)
      if(Utils.envVar("TEST_MODE") != "False" && args.length>1){
        if(args(1).nonEmpty){
           DechetAnalysis.setNow(Instant.parse(args(1)))
        }
        
      }
    } catch {
      case e: Throwable => {
        println("Des arguments manquent")
        println("Commande lancee :")
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

    /** Chargement des paramètres Hadoop à partir des proprietes système */
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
    val env =if(sys.env.get("ENV")==None)"unknown" else Some(sys.env.get("ENV")).toString()
    try {
      createEmptyBacDataFrame(spark,SYSDATE).show()
      val t0 = System.nanoTime()
      println("Master :" + spark.sparkContext.master)
      println("env : " + System.getenv().toString)

      //import du dernier referentiel historise
      val df_lastestProducteur = ImportDechet.readLastestReferential(spark, SYSDATE, nameEnvProd)//.repartition(400,col("code_producteur"))
      println("LATEST PRODUCTEUR REF SHOW")
      df_lastestProducteur.show()
      val df_lastestBac = ImportDechet.readLastestReferential(spark, SYSDATE, nameEnvRecip)//.repartition(400,col("code_puce"))
      println("LATEST BAC REF SHOW")
      df_lastestBac.show()
      val t1 = System.nanoTime()
      println("Elapsed time before join: " + (t1 - t0)/1000000000 + "s (" + (t1 - t0) + " ns)")
      val join = df_lastestProducteur.join(df_lastestBac,Seq("code_producteur"),"inner")
      println("Jointure prod x bac")
      join.show()
      val t2 = System.nanoTime()
      println("Elapsed time for join: " + (t2 - t1) / 1000000000 + "s (" + (t2 - t1) + " ns)")
      println("Total elapsed time: " + (t2 - t0)/1000000000 + "s (" + (t2 - t0) + " ns)")
    } catch {
      case e: Throwable => {
        logger.error("Echec du traitement d'analyse Dechet")
        throw new Exception("Echec du traitement d'analyse Dechet", e)
      }
    }
  }
}
