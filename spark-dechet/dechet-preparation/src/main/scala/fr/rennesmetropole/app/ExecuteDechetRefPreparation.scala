package fr.rennesmetropole.app

import com.typesafe.scalalogging.Logger
import fr.rennesmetropole.services.{DechetRefPreparation, ImportDechetRef}
import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExecuteDechetRefPreparation {
  def main(args: Array[String]): Either[Unit, DataFrame] = {
    val logger = Logger(getClass.getName)

    /** SYSDATE recupere la date actuelle de l'horloge systeme dans le fuseau horaire par defaut (UTC) */
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
        println("Commande lancee :")
        println("spark-submit --class fr.rennesmetropole.app.ExecuteDechetRefPreparation /app-dechet/target/rm-dechet-preparation-1.0-SNAPSHOT.jar <DATE> <csv>")
        println("DATE : 2021-05-07 => yyyy/mm/dd")
        println("csv : csv/true => active l'ecriture d'un fichier au format csv en plus du fichier ORC")
        throw new Exception("Pas d'arguments", e )
      }
    }

    /** Initialisation de la session spark */
    val spark: SparkSession = SparkSession.builder()
      .appName("Referentiel Dechet Ref Preparation")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
     
      .getOrCreate()

    /** Chargement des parametres Hadoop à partir des proprietes systeme */
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
    var bacPrepareException = false
    var prodImportException = false
    var prodPrepareException = false
    try {
      val df_ImportDechetRefProducer = ImportDechetRef.ExecuteImportDechetRef(spark, SYSDATE, nameEnvProd)
      println("IMPORT DECHET REFERENTIEL DES PRODUCTEURS")
      df_ImportDechetRefProducer.show(false)
      println("IMPORT DECHET Ref Producteur END")
      val df_ImportDechetRefRecip = ImportDechetRef.ExecuteImportDechetRef(spark, SYSDATE, nameEnvRecip)
      println("IMPORT DECHET REFERENTIEL DES RECIPIENTS PUCES")
      df_ImportDechetRefRecip.show(false)


      var df_PreparationDechetRefProducer = df_ImportDechetRefProducer
      if(!df_ImportDechetRefProducer.head(1).isEmpty){
        df_PreparationDechetRefProducer = DechetRefPreparation.ExecuteDechetRefPreparation(spark, df_ImportDechetRefProducer, nameEnvProd)
        println("PREPARED REFERENTIEL DECHET DES PRODUCTEURS")
        df_PreparationDechetRefProducer.show(false)
      }else {
        prodImportException = true
        logger.error("Pas de donnee Producteur importe, skip de la preparation Producteur")
      }

      var df_PreparationDechetRefRecip = df_ImportDechetRefRecip
      if(!df_ImportDechetRefRecip.head(1).isEmpty){
        df_PreparationDechetRefRecip = DechetRefPreparation.ExecuteDechetRefPreparation(spark, df_ImportDechetRefRecip, nameEnvRecip)
        println("PREPARED REFERENTIEL DECHET DES RECIPIENTS PUCES")
        df_PreparationDechetRefRecip.show(false)
      }else {
        bacImportException = true
        logger.error("Pas de donnee Bac importe, skip de la preparation Bac")
      }


      if (Utils.envVar("TEST_MODE") == "False" && (df_PreparationDechetRefProducer.head(1).isEmpty)) {
        prodPrepareException = true
      }
      if (Utils.envVar("TEST_MODE") == "False" && (df_PreparationDechetRefRecip.head(1).isEmpty)) {
        logger.error("Pas de donnees bacs a ecrire sur minio...")
        bacPrepareException = true
      }
      var exception ="";
      // Exception a l'importation
      if(bacImportException && prodImportException){
        exception = exception + "ERROR : pas de donnees referentiel producteur et bac dans minio \n"
      }else if (bacImportException && !prodImportException){
        exception = exception + "ERROR : pas de donnees referentiel bacs dans minio \n"
      }else if (!bacImportException && prodImportException){
        exception = exception + "ERROR : pas de donnees referentiel producteur dans minio \n"
      }
      // Exception a la preparation
      if(bacPrepareException && prodPrepareException){
         exception = exception + "ERROR : pas de donnees referentiel bacs et producteur recupere, se referer aux logs pour savoir si le probleme viens de la preparation ou de la lecture "
      }else if (bacPrepareException && !prodPrepareException){
         exception = exception + "ERROR : pas de donnees referentiel bacs recupere, se referer aux logs pour savoir si le probleme viens de la preparation ou de la lecture "
      }else if (!bacPrepareException && prodPrepareException){
        exception = exception + "ERROR : pas de donnees referentiel producteur recuperee, se referer aux logs pour savoir si le probleme viens de la preparation ou de la lecture "
      }
      if(exception != ""){
        logger.error(exception)
        throw new Exception(exception)
      }

       if (Utils.envVar("TEST_MODE") == "False" && (!df_PreparationDechetRefProducer.head(1).isEmpty)) {
        Left(Utils.writeToS3(spark,df_PreparationDechetRefProducer, nameEnvProd,csv,SYSDATE))
      }
      else {
        Right(df_PreparationDechetRefProducer)
      }

      if (Utils.envVar("TEST_MODE") == "False" && (!df_PreparationDechetRefRecip.head(1).isEmpty)) {
        Left(Utils.writeToS3(spark,df_PreparationDechetRefRecip, nameEnvRecip,csv,SYSDATE))
      }
      else {
        Right(df_PreparationDechetRefRecip)
      }

    } catch {
      case e: Throwable => {
        logger.error("Echec de la preparation du referentiel Dechet")
        throw new Exception("ERROR : Echec de la preparation du referentiel Dechet", e)
      }
    }
  }
}
