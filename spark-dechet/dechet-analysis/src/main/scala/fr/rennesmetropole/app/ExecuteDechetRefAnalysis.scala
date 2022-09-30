package fr.rennesmetropole.app


import fr.rennesmetropole.services.MailAgent.{mail_template, sendMail}
import fr.rennesmetropole.services.{DechetAnalysis, ImportDechet}
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.{logger, show}
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import fr.rennesmetropole.tools.Utils.log
import java.time.Instant


object ExecuteDechetRefAnalysis {
  def main(args: Array[String]):  Either[Unit, (DataFrame,DataFrame)] = {
    /** SYSDATE recupère la date actuelle de l'horloge système dans le fuseau horaire par defaut (UTC) */
    log(" **** ExecuteDechetRefAnalysis ***** ")
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
      log("Master :" + spark.sparkContext.master)
      log("env : " + System.getenv().toString)

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
        val content = mail_template(this.getClass.getName, env,exception_verif)
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
        Right(df_AnalysedDechetProducteur,df_AnalysedDechetBac)
      }
      else { //partie tests
        if (!df_AnalysedDechetBac.isEmpty && !df_AnalysedDechetProducteur.isEmpty) {
          val path = "src/test/resources/Local/app/ExecuteDechetRefAnalysis/Input/"
          df_AnalysedDechetBac = df_AnalysedDechetBac.coalesce(1)
          df_AnalysedDechetProducteur = df_AnalysedDechetProducteur.coalesce(1)
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
          df_AnalysedDechetBac.write.options(Map("header" -> "true", "delimiter" -> ";")).mode(SaveMode.Overwrite).csv(path + "latest_ref_bac/csv/")
          df_AnalysedDechetProducteur.write.options(Map("header" -> "true", "delimiter" -> ";")).mode(SaveMode.Overwrite).csv(path + "latest_ref_prod/csv/")
          var list_bac = new java.io.File(path + "latest_ref_bac/csv/").listFiles.filter(_.getName.endsWith(".csv"))
          var list_prod = new java.io.File(path + "latest_ref_prod/csv/").listFiles.filter(_.getName.endsWith(".csv"))
          val file_bac = list_bac.mkString("").split("\\\\")
          val file_prod = list_prod.mkString("").split("\\\\")
          fs.rename(new Path(path + "latest_ref_prod/csv/" + file_prod(file_prod.length - 1)), new Path(path + "latest_ref_prod/csv/ref_prod.csv"))
          fs.rename(new Path(path + "latest_ref_bac/csv/" + file_bac(file_bac.length - 1)), new Path(path + "latest_ref_bac/csv/ref_bac.csv"))
          fs.delete(new Path(path + "latest_ref_bac/csv/.ref_bac.csv.crc"), true)
          fs.delete(new Path(path + "latest_ref_prod/csv/.ref_prod.csv.crc"), true)
          fs.delete(new Path(path + "latest_ref_bac/csv/._SUCCESS.crc"), true)
          fs.delete(new Path(path + "latest_ref_prod/csv/._SUCCESS.crc"), true)

          df_AnalysedDechetBac.write.mode(SaveMode.Append).orc(path + "latest_ref_bac/orc/")
          df_AnalysedDechetProducteur.write.mode(SaveMode.Append).orc(path + "latest_ref_prod/orc/")
          log("DELETE OLD ORC")
          fs.delete(new Path(path + "latest_ref_bac/orc/ref_bac.orc"), true)
          fs.delete(new Path(path + "latest_ref_prod/orc/ref_prod.orc"), true)
          var list_bac_orc = new java.io.File(path + "latest_ref_bac/orc/").listFiles.filter(_.getName.endsWith(".orc"))
          var list_prod_orc = new java.io.File(path + "latest_ref_prod/orc/").listFiles.filter(_.getName.endsWith(".orc"))
          val file_bac_orc = list_bac_orc.mkString("").split("\\\\")
          val file_prod_orc = list_prod_orc.mkString("").split("\\\\")
          fs.rename(new Path(path + "latest_ref_prod/orc/" + file_prod_orc(file_prod_orc.length - 1)), new Path(path + "latest_ref_prod/orc/ref_prod.orc"))
          fs.rename(new Path(path + "latest_ref_bac/orc/" + file_bac_orc(file_bac_orc.length - 1)), new Path(path + "latest_ref_bac/orc/ref_bac.orc"))
          fs.delete(new Path(path + "latest_ref_bac/orc/.ref_bac.orc.crc"), true)
          fs.delete(new Path(path + "latest_ref_prod/orc/.ref_prod.orc.crc"), true)
          fs.delete(new Path(path + "latest_ref_bac/orc/._SUCCESS.crc"), true)
          fs.delete(new Path(path + "latest_ref_prod/orc/._SUCCESS.crc"), true)
          Right(df_AnalysedDechetBac,df_AnalysedDechetProducteur)
        }else {
          logger.error(exception)
          Right(df_AnalysedDechetBac,df_AnalysedDechetProducteur)
        }

      }
    } catch {
      case e: Throwable => {
        logger.error("Echec du traitement d'analyse Dechet")
        throw new Exception("Echec du traitement d'analyse Dechet", e)
      }
    }
  }
}
