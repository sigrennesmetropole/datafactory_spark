package fr.rennesmetropole.app

import fr.rennesmetropole.services.MailAgent.{mail_template, sendMail}
import fr.rennesmetropole.services.{DechetAnalysis, ImportDechet}
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.{show,logger,log}
import org.apache.hadoop.fs._
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import fr.rennesmetropole.tools.Utils.log
import java.time.Instant


object ExecuteDechetRefAnalysisBac {
  def main(args: Array[String]):  Either[Unit,DataFrame] = {
    log(" **** ExecuteDechetRefAnalysisBac ***** ")
    log("LOG LEVEL " + logger.getLevel)
    
    /** SYSDATE recupère la date actuelle de l'horloge système dans le fuseau horaire par defaut (UTC) */
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
        log("Des arguments manquent")
        log("Commande lancee :")
        log("spark-submit --class fr.rennesmetropole.app.ExecuteDechetRefAnalysis /app-dechet/target/rm-dechet-analysis-1.0-SNAPSHOT.jar <DATE>")
        log("DATE : 2021-05-07 => yyyy/mm/dd")
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
    var bacImportException = false
    var bacAnalyseException = false
    var bacVerifException = false
    var prodVerifException = false
    var prodImportException = false
    var prodAnalyseException = false
    var exception ="";
    var exception_verif ="";
    val env =if(sys.env.get("ENV")==None)"unknown" else Some(sys.env.get("ENV")).toString()
    try {
      log("Master :" + spark.sparkContext.master)
      log("env : " + System.getenv().toString)

      //import du dernier referentiel historise
      val df_lastestBac = ImportDechet.readLastestReferential(spark, SYSDATE, nameEnvRecip).repartition(400,col("code_puce"))
      show(df_lastestBac,"FUNCTION LATEST")
      //Import des producteurs analysé
      val df_AnalysedDechetProducteur = Utils.readDataPath(spark, SYSDATE, nameEnvProd,"analysed_bucket","smart_orc")
      show(df_AnalysedDechetProducteur,"Donnée producteur déja analysé")

      // Traitement des bacs
      var df_AnalysedDechetBac :DataFrame = DechetAnalysis.createEmptyBacDf(spark)
      //import du referentiel producteur a integrer
      var df_ImportDechetBac = ImportDechet.ExecuteImportDechet(spark, SYSDATE, nameEnvRecip).repartition(400,col("code_puce"))
      show(df_ImportDechetBac,"IMPORT DECHET Ref Bac")
      if(!df_ImportDechetBac.isEmpty){
        val tuples_bac =  ImportDechet.verif(spark, df_ImportDechetBac, nameEnvRecip)
        df_ImportDechetBac = tuples_bac._1
        exception_verif = exception_verif + tuples_bac._2

        if(!df_ImportDechetBac.isEmpty){
          val datePhoto: String = df_ImportDechetBac.first().getAs("date_photo")
          // Traitement d'enrichissement des bacs
          df_AnalysedDechetBac = DechetAnalysis.ExecuteDechetAnalysis_Bac(spark, df_ImportDechetBac,SYSDATE,
            datePhoto, df_lastestBac, df_AnalysedDechetProducteur)
            val df_AnalysedDechetBac_temp = df_AnalysedDechetBac.dropDuplicates("id_bac")
          //log("ligne bac supprimé par le dropDuplicate")
          //df_AnalysedDechetBac.union(df_AnalysedDechetBac_temp).except(df_AnalysedDechetBac.intersect(df_AnalysedDechetBac_temp)).show()
          df_AnalysedDechetBac = df_AnalysedDechetBac_temp
          show(df_AnalysedDechetBac,"ANALYSE BAC FINAL")

        }
        else {
          bacVerifException = true
        }
        // Recuperation de la date photo


      }else{
        bacImportException=true
      }

       if (Utils.envVar("TEST_MODE") == "False" && (df_AnalysedDechetProducteur.head(1).isEmpty)) {
        prodAnalyseException = true
      }
      if (Utils.envVar("TEST_MODE") == "False" && (df_AnalysedDechetBac.head(1).isEmpty)) {
        bacAnalyseException = true
      }
      //envoie du mail si doublons trouver
      if(exception_verif!=""){
        val content = mail_template(this.getClass.getName, env,exception_verif,SYSDATE)
        sendMail(content,"Alerte spark - verif doublons")
      }

      // Exception a l'importation
      if(bacImportException && prodImportException){
        exception = exception + "ERROR : pas de donnees referentiel historise producteur et bac dans minio \n"
      }else if (bacImportException && !prodImportException){
        exception = exception + "ERROR : pas de donnees referentiel historise bacs dans minio \n"
      }else if (!bacImportException && prodImportException){
        exception = exception + "ERROR : pas de donnees referentiel historise producteur dans minio \n"
      }
      // Exception a l'analyse
      if(bacAnalyseException && prodAnalyseException){
         exception = exception + "ERROR : pas de donnees referentiel bacs et producteur recupere, se referer aux logs pour savoir si le probleme viens de l'analyse ou de la lecture "
      }else if (bacAnalyseException && !prodAnalyseException){
         exception = exception + "ERROR : pas de donnees referentiel bacs recupere, se referer aux logs pour savoir si le probleme viens de l'analyse ou de la lecture "
      }else if (!bacAnalyseException && prodAnalyseException){
        exception = exception + "ERROR : pas de donnees referentiel producteur recuperee, se referer aux logs pour savoir si le probleme viens de l'analyse ou de la lecture "
      }
      logger.error("exception :" + exception)
      if(exception != "" && Utils.envVar("TEST_MODE") == "False"){
        throw new Exception(exception)
      }

      if (Utils.envVar("TEST_MODE") == "False"){
        if(!df_AnalysedDechetProducteur.head(1).isEmpty){
          Left(Utils.writeToS3(spark,df_AnalysedDechetProducteur,nameEnvProd,SYSDATE))
         }
        
        if(!df_AnalysedDechetBac.head(1).isEmpty){
          Left(Utils.writeToS3(spark,df_AnalysedDechetBac,nameEnvRecip,SYSDATE))
        }
        Right(df_AnalysedDechetBac)
      }
      else {
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
