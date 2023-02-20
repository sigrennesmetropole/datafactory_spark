package fr.rennesmetropole.app


import fr.rennesmetropole.services.MailAgent.{mail_template, sendMail}
import fr.rennesmetropole.services.{DechetAnalysis, ImportDechet}
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.{log, logger, show}
import org.apache.hadoop.fs._
import org.apache.log4j.Level
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import fr.rennesmetropole.tools.Utils.log
import java.time.Instant


object ExecuteDechetRefAnalysisProd {
  def main(args: Array[String]):  Either[Unit, DataFrame] = {
    /** SYSDATE recupère la date actuelle de l'horloge système dans le fuseau horaire par defaut (UTC) */
    log(" **** ExecuteDechetRefAnalysisProd ***** ")
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
    log("env : " + System.getenv().toString)
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

      //import du dernier referentiel historise
      val df_lastestProducteur = ImportDechet.readLastestReferential(spark, SYSDATE, nameEnvProd).repartition(400,col("code_producteur"))
      log("PRODUCTEUR SHOW")
      df_lastestProducteur.show()
      val df_lastestBac = ImportDechet.readLastestReferential(spark, SYSDATE, nameEnvRecip).repartition(400,col("code_puce"))
      show(df_lastestProducteur,"FUNCTION LATEST")

      //import du referentiel producteur a integrer
      var df_ImportDechetProducteur = ImportDechet.ExecuteImportDechet(spark, SYSDATE, nameEnvProd).repartition(400,col("code_producteur"))
      show(df_ImportDechetProducteur,"IMPORT DECHET Ref Producteur")
      var df_AnalysedDechetProducteur :DataFrame = DechetAnalysis.createEmptyProducteurDf(spark)
      if(!df_ImportDechetProducteur.isEmpty){
        val tuples_prod =  ImportDechet.verif(spark, df_ImportDechetProducteur, nameEnvProd)
        df_ImportDechetProducteur = tuples_prod._1
        exception_verif = exception_verif + tuples_prod._2
        log("df_ImportDechetProducteur après verif")
        df_ImportDechetProducteur.show()
        if(!df_ImportDechetProducteur.isEmpty){
          val datePhoto: String = df_ImportDechetProducteur.first().getAs("date_photo")

          df_AnalysedDechetProducteur = DechetAnalysis.ExecuteDechetAnalysis_Producteur(spark,
            df_ImportDechetProducteur, SYSDATE, datePhoto, df_lastestProducteur)
          val df_AnalysedDechetProducteur_tmp = df_AnalysedDechetProducteur.dropDuplicates("id_producteur")
          //log("ligne producteur supprimé par le dropDuplicate")
          //df_AnalysedDechetProducteur.union(df_AnalysedDechetProducteur_tmp).except(df_AnalysedDechetProducteur.intersect(df_AnalysedDechetProducteur_tmp)).show(10000)
          df_AnalysedDechetProducteur = df_AnalysedDechetProducteur_tmp
          show(df_AnalysedDechetProducteur,"ANALYSE PRODUCTEUR FINAL")

        }
        else {
          prodVerifException = true
        }
        // Recuperation de la date photo


      }else{
        prodImportException = true
      }



       if (Utils.envVar("TEST_MODE") == "False" && (df_AnalysedDechetProducteur.head(1).isEmpty)) {
        prodAnalyseException = true
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

        Right(df_AnalysedDechetProducteur)
      }else {
        Right(df_AnalysedDechetProducteur)
      }

    } catch {
      case e: Throwable => {
        logger.error("Echec du traitement d'analyse Dechet")
        throw new Exception("Echec du traitement d'analyse Dechet", e)
      }
    }
  }
}
