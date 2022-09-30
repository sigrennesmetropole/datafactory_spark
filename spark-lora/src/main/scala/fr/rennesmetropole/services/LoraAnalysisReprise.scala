package fr.rennesmetropole.services
import fr.rennesmetropole.tools.Utils
import fr.rennesmetropole.tools.Utils.{log, show}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate
object LoraAnalysisReprise {


  def getSensor(spark: SparkSession): DataFrame = {
    if(Utils.envVar("TEST_MODE") == "False") {
      Utils.readFomPostgres(spark, Utils.envVar("POSTGRES_URL"), Utils.envVar("POSTGRES_TABLE_SENSOR_NAME"))
    }else {
      Utils.readFomPostgres(spark,"","")
    }
  }
  def capteurAReprendre(df_sensor:DataFrame):DataFrame ={
    df_sensor.filter(col("updatedon")>=col("reprisedon") or
      (col("updatedon").isNotNull and col("reprisedon").isNull))
      .select("deveui","enabledon","disabledon","updatedon","reprisedon")
  }
  def getDateBetween(date_start:String,date_end:String):Seq[String]={
    //https://codereview.stackexchange.com/questions/44849/construct-date-sequence-in-scala
    var list = Seq[String]()
    LoraAnalysisReprise.dayIterator(LocalDate.parse(date_start), LocalDate.parse(date_end)).foreach(date => {
      list = list :+ date.toString
    })
    list
  }
  def dayIterator(start: LocalDate, end: LocalDate) = Iterator.iterate(start)(_ plusDays 1) takeWhile (_ isBefore end)
}
