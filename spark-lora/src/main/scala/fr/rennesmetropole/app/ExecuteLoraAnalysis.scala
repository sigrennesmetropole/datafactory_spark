package fr.rennesmetropole.app

import com.typesafe.config.ConfigFactory
import fr.rennesmetropole.services.{ImportTrameLora, LoraAnalysis}
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.{config, date2URL, getClass, log, show}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, from_json, monotonically_increasing_id}
import org.apache.spark.sql.types._

import java.time.{Duration, Instant}
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}
import org.apache.logging.log4j.LogManager

object ExecuteLoraAnalysis {
  val logger = LogManager.getLogger(getClass.getName)
  var config = ConfigFactory.load()
  def main(args: Array[String]): Either[Unit, DataFrame] = {


    /** SYSDATE recupère la date actuelle de l'horloge système dans le fuseau horaire par défaut (UTC) */
    var SYSDATE = java.time.LocalDate.now.toString
    try {
      SYSDATE = args(0)
    } catch {
      case e: Throwable => {
        println("Des arguments manquent")
        println("Commande lancée :")
        println("spark-submit --class fr.rennesmetropole.app.ExecuteLoraAnalysis /app-lora/target/rm-lora-analysis-1.0-SNAPSHOT.jar <DATE>")
        println("DATE : 2021-05-07 => yyyy/mm/dd")
        throw new Exception("Pas d'arguments", e )
      }
    }

    /** Initialisation de la session spark */
    val spark: SparkSession = SparkSession.builder()
      .appName("Lora Analysis")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.sql.caseSensitive", "true")
      .getOrCreate()

    log("ExecuteLoraAnalysis")
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

    try {
      val start = Instant.now
      val conf = spark.sparkContext.hadoopConfiguration
      val path = Utils.envVar("READ_URL")+date2URL(SYSDATE)
      var allSucces = true
      var listDeveuiEchec: Seq[String] = Seq()
      val df_sensor = Utils.readFomPostgres(spark,Utils.envVar("POSTGRES_URL"), Utils.envVar("POSTGRES_TABLE_SENSOR_NAME"))
      var listDeveui = Set[String]()
      for (r <- df_sensor.rdd.collect) {
        listDeveui += r.getAs("deveui")
      }
      new Path(path).getFileSystem(conf).listStatus(new Path(path)).filter(_.isDirectory).map(_.getPath)
        .foreach(result => {
          val deveui = result.getName
          val df_ImportLora = ImportTrameLora.ExecuteImportLora(spark, SYSDATE, deveui)
/*          try {
            if (!df_ImportLora.head(1).isEmpty) {
              val jsondf = df_ImportLora.toJSON
              var brutData = jsondf.withColumn("values", from_json(col("value"), df_ImportLora.schema, Map[String, String]().asJava))
              brutData = brutData.withColumn("timestamp", col("values.timestamp").cast(TimestampType))
                .withColumn("deveui", col("values.deveui"))
                .withColumn("id", monotonically_increasing_id())
                .drop("values")
              var partitionedBrutDf = Utils.dfToPartitionedDf(brutData, SYSDATE)
              partitionedBrutDf = partitionedBrutDf.drop("technical_key")
              Utils.postgresPersist(spark, Utils.envVar("POSTGRES_URL"), partitionedBrutDf,
                Utils.envVar("POSTGRES_TABLE_LORA_BRUT_NAME"), SYSDATE, deveui)
            }
          } catch {
            case _ => {
              println("Echec de l'insertion des données brutes pour le capteur " + deveui)
            }
          }*/
          if (listDeveui.contains(deveui)) {
            val schema = StructType(
              List(
                StructField("channel", StringType, false),
                StructField("deveui", StringType, false),
                StructField("enabledon", StringType, false),
                StructField("disabledon", StringType, false),
                StructField("measurenatureid", StringType, false),
                StructField("unitid", StringType, false),
                StructField("dataparameter", StringType, false)
              )
            )

            var partitionedDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
            breakable {
              try {
                /** Retourne le dataframe après lecture de la donnée dans Minio */
                // val df_ImportLora = ImportLora.ExecuteImportLora(spark, SYSDATE, deveui)
                println("-- df imported --")
                df_ImportLora.show(false)

                if (!df_ImportLora.head(1).isEmpty) {
                  /** Retourne le dataframe après analyse */
                  val df_LoraAnalyzed = LoraAnalysis.ExecuteLoraAnalysis(spark, df_ImportLora, deveui)

                  if (!df_LoraAnalyzed.head(1).isEmpty) {
                    /** Retourne le dataframe après enrichissement */
                    val df_LoraEnrichi = LoraAnalysis.enrichissement_Basique(spark, df_LoraAnalyzed)
                    println("-- df pre postgres --")
                    df_LoraEnrichi.show(false)

                    partitionedDf = Utils.dfToPartitionedDf(df_LoraEnrichi, SYSDATE)
                  } else {
                    println("Aucune données après l'analyse, peut être que le référentiel est incomplet...")
                  }

                } else {
                  println("Aucune donnee importe, skip du traitement...")
                }

              } catch {
                case e: Throwable => {
                  println("Echec du traitement d'analyse LoRa", e)
                  e.printStackTrace()
                  allSucces = false
                  listDeveuiEchec = listDeveuiEchec :+ deveui
                  break
                }

              }

              /** Donne les accès pour se connecter à Postgresql et procède à l'écriture en mode APPEND s'il y a eu enrichissement */
              if (Utils.envVar("TEST_MODE") == "False" && (!partitionedDf.head(1).isEmpty)) {
                Left(Utils.postgresPersist(spark, Utils.envVar("POSTGRES_URL"), partitionedDf, Utils.envVar("POSTGRES_TABLE_LORA_NAME"), SYSDATE, deveui))
              }
              else {
                Right(partitionedDf)
              }
            }
          }else {
            println("Capteur "+deveui+" non configuré pour l'enrichissement")
          }
        })
      val end = Instant.now
      val time = Duration.between(start, end)
      println("Insertion des données brutes en "+time)
      if(!listDeveuiEchec.isEmpty) {
        println("Capteurs en echec " + listDeveuiEchec.mkString(","))
      }
      Left(allSucces)
    }catch {
      case e: Throwable => {
        logger.error("Echec du traitement d'analyse LoRa")
        throw new Exception("Echec du traitement d'analyse LoRa", e)
      }
    }

    //try {
/*      val df_sensor = Utils.readFomPostgres(spark,Utils.envVar("POSTGRES_URL"), Utils.envVar("POSTGRES_TABLE_SENSOR_NAME"))
      var listDeveui = Set[String]()
      for (r <- df_sensor.rdd.collect) {
        listDeveui += r.getAs("deveui")
      }

      val schema = StructType(
        List(
          StructField("channel", StringType, false),
          StructField("deveui", StringType, false),
          StructField("enabledon", StringType, false),
          StructField("disabledon", StringType, false),
          StructField("measurenatureid", StringType, false),
          StructField("unitid", StringType, false),
          StructField("dataparameter", StringType, false)
        )
      )

      var allSucces = true
      var listDeveuiEchec: Seq[String] = Seq()
      listDeveui.foreach {
        case deveui =>
          var partitionedDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
          breakable {
            try {
              /** Retourne le dataframe après lecture de la donnée dans Minio */
              val df_ImportLora = ImportLora.ExecuteImportLora(spark, SYSDATE, deveui)
              println("-- df imported --")
              df_ImportLora.show(false)

              if (!df_ImportLora.head(1).isEmpty) {
                /** Retourne le dataframe après analyse */
                val df_LoraAnalyzed = LoraAnalysis.ExecuteLoraAnalysis(spark, df_ImportLora, deveui)

                if (!df_LoraAnalyzed.head(1).isEmpty) {
                  /** Retourne le dataframe après enrichissement */
                  val df_LoraEnrichi = LoraAnalysis.enrichissement_Basique(spark, df_LoraAnalyzed)
                  println("-- df pre postgres --")
                  df_LoraEnrichi.show(false)

                  partitionedDf = Utils.dfToPartitionedDf(df_LoraEnrichi, SYSDATE)
                } else {
                  println("Aucune données après l'analyse, peut être que le référentiel est incomplet...")
                }

              } else {
                println("Aucune donnee importe, skip du traitement...")
              }

            } catch {
              case e: Throwable => {
                println("Echec du traitement d'analyse LoRa", e)
                e.printStackTrace()
                allSucces = false
                listDeveuiEchec = listDeveuiEchec :+ deveui
                break
              }

            }
            /** Donne les accès pour se connecter à Postgresql et procède à l'écriture en mode APPEND s'il y a eu enrichissement */
            if (Utils.envVar("TEST_MODE") == "False" && (!partitionedDf.head(1).isEmpty)) {
              Left(Utils.postgresPersist(spark, Utils.envVar("POSTGRES_URL"), partitionedDf, Utils.envVar("POSTGRES_TABLE_LORA_NAME"), SYSDATE, deveui))
            }
            else {
              Right(partitionedDf)
            }
          }
      }*/
     /* if(!listDeveuiEchec.isEmpty) {
        println("Capteurs en echec " + listDeveuiEchec.mkString(","))
      }
    Left(allSucces)
    } catch {
      case e: Throwable => {
        logger.error("Echec du traitement d'analyse LoRa")
        throw new Exception("Echec du traitement d'analyse LoRa", e)
      }
    }*/
  }
}
