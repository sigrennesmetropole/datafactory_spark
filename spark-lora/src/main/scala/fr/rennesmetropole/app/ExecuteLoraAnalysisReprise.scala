package fr.rennesmetropole.app

import fr.rennesmetropole.services.{ImportTrameLora, LoraAnalysis, LoraAnalysisReprise}
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.{date2URL, getClass, log, show}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

import java.time.{Duration, Instant, LocalDate}
import scala.util.control.Breaks.{break, breakable}

object ExecuteLoraAnalysisReprise {
  def main(args: Array[String]): Either[Unit, DataFrame] = {
    val logger = LogManager.getLogger(getClass.getName)

    /** SYSDATE recupère la date actuelle de l'horloge système dans le fuseau horaire par défaut (UTC) */
    var SYSDATE = java.time.LocalDate.now.toString
    try {
      SYSDATE = args(0)
    } catch {
      case e: Throwable => {
        println("Des arguments manquent")
        println("Commande lancée :")
        println("spark-submit --class fr.rennesmetropole.app.ExecuteLoraAnalysis /app-lora/target/rm-lora-analysis.jar ")
        throw new Exception("Pas d'arguments", e )
      }
    }

    /** Initialisation de la session spark */
    val spark: SparkSession = SparkSession.builder()
      .appName("Lora Analysis")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.sql.caseSensitive", "true")
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

    try {
      val start = Instant.now
      var allSucces = true
      var listDeveuiEchec: Seq[String] = Seq()
      val df_sensor = LoraAnalysisReprise.getSensor(spark)

      show(df_sensor,"df_sensor")
      val df_a_reprendre = LoraAnalysisReprise.capteurAReprendre(df_sensor)
      show(df_a_reprendre,"df_a_reprendre")
      if(!df_a_reprendre.head(1).isEmpty){
        var listDeveui = Set[String]()
        for (r <- df_sensor.rdd.collect) {
          listDeveui += r.getAs("deveui")
        }
        val map = df_a_reprendre.rdd.map(row =>{
          row.get(0)->(if(row.get(2)==null) LoraAnalysisReprise.getDateBetween(row.get(1).toString,SYSDATE) else LoraAnalysisReprise.getDateBetween(row.get(1).toString,row.get(2).toString))

        }).collectAsMap()
        log(map.mkString(" \n"))
        map.foreach(result => {
          val deveui = result._1.toString
          println("NAME :" + deveui)

            if (listDeveui.contains(deveui)) {

              var df_ImportLora = spark.emptyDataFrame
              result._2.foreach(date => {
                val df_temp = ImportTrameLora.ExecuteImportLora(spark, date, deveui)
                if(!df_temp.head(1).isEmpty) {
                  df_ImportLora = df_ImportLora.unionByName(df_temp, allowMissingColumns = true)
                  df_ImportLora = Utils.dfToPrePartitionedDf_Reprise(df_ImportLora, date)
                }
              })

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

                      partitionedDf = Utils.dfToPostPartitionedDf_Reprise(df_LoraEnrichi)
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
                  Utils.postgresPersist_Reprise(spark, Utils.envVar("POSTGRES_URL"), partitionedDf, Utils.envVar("POSTGRES_TABLE_LORA_NAME"), map.get(deveui).get.head,  map.get(deveui).get.last, deveui) // head => date_debut last => date de fin
                  Left(Utils.updateSensor_Reprise(  Utils.envVar("POSTGRES_TABLE_SENSOR_NAME"), Utils.envVar("POSTGRES_URL"), SYSDATE, deveui))
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
        if(listDeveuiEchec.nonEmpty) {
          println("Capteurs en echec " + listDeveuiEchec.mkString(","))
        }
      }
      else {
        logger.error("Echec du traitement d'analyse LoRa")
        throw new Exception("Pas de donne a reprendre")
      }
      Left(allSucces)

Right(df_a_reprendre)
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
