package fr.rennesmetropole.app

import fr.rennesmetropole.services.{AnalyseTraffic, ImportTraffic}
import fr.rennesmetropole.tools.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}


object ExecuteTrafficAnalysis {
  def main(args: Array[String]): Either[Unit, DataFrame] = {
    /** traitement des paramètres WINDOW et DATE */
    var WINDOW = Utils.envVar("WINDOW").toInt
    var DATE = java.time.LocalDate.now.toString // by default we put the current time in UTC
    if (args.isEmpty) {
      println("Arguments are missing")
      println("Command :")
      println("spark-submit --class fr.rennesmetropole.app.ExecuteTrafficAnalysis /app/target/pfdata-spark-dataproc-1.0-SNAPSHOT.jar <DATE> <WINDOW> ")
      println("WINDOW : taille de la fenêtre en minutes, par défaut à 15")
      println("DATE : 2021-05-07 => yyyy/mm/dd")

      sys.exit(1)
    }
    if (args.length == 2) {
      WINDOW = args(1).toInt
    }

    DATE = args(0)
    val URL = Utils.envVar("READ_URL")
    val FORMAT = Utils.envVar("READ_FORMAT")
    /** Initialisation de la session spark */
    val spark: SparkSession = SparkSession.builder()
      .appName("Traffic Analysis")
       .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate();
    //spark.sparkContext.setLogLevel("debug")
    spark.sparkContext.hadoopConfiguration.set("fs.s3.aws.credentials.provider", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", Utils.envVar("S3_ENDPOINT"));
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Utils.envVar("S3_ACCESS_KEY"));
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Utils.envVar("S3_SECRET_KEY"));
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true");
    

    val importTrafficDf = ImportTraffic.ExecuteImportTraffic(spark,DATE)

    val analyzedDf = AnalyseTraffic.ExecuteAnalyseTraffic(spark, importTrafficDf, WINDOW)
    val partitionedDf = Utils.dfToPartitionedDf(analyzedDf, DATE)
    partitionedDf.show(10)
    /** Ecriture de la donnée dans postgres */
    if((Utils.envVar("TEST_MODE") == "False") && (!partitionedDf.head(1).isEmpty)) {
      Left(Utils.postgresPersist(spark, Utils.envVar("POSTGRES_URL"), partitionedDf, Utils.envVar("POSTGRES_TABLE_TRAFFIC_NAME"), DATE))
    }else{
      Right(partitionedDf)
    }
}

}
