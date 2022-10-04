  package fr.rennesmetropole.tools

  import com.typesafe.config.{Config, ConfigFactory}
  import com.typesafe.scalalogging.Logger
  import org.apache.commons.lang3.StringUtils
  import org.apache.hadoop.fs.Path
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._

  import scala.collection.JavaConversions._

  object Utils {

    val logger = Logger(getClass.getName)
    var config = ConfigFactory.load()
    //var URL = Utils.envVar("READ_URL")



    /**
    *
    * @param name : nom de la configuration
    * @param no_error
    * @param default
    * @return
    */
    def envVar(name: String, no_error: Boolean = false, default: String = ""): String = {
      var value: String = config.getString("env." + name)
      if (value == null && (no_error == null || no_error == false)) {
        throw new Exception("Env var '" + name + "' must be defined")
      }
      if (value == null && default != null) {
        default
      }
      value
    }

    /**
    *
    * @param name : nom de la configuration
    * @param no_error
    * @param default
    * @return
    */
    def tableVar(nameEnv:String, name: String, no_error: Boolean = false, default: String = ""): String = {
      var value: String = config.getString(nameEnv + "." + name)
      if (value == null && (no_error == null || no_error == false)) {
        throw new Exception(nameEnv+" var '" + name + "' must be defined")
      }
      if (value == null && default != null) {
        default
      }
      value
    }


    /**
    *
    * @param spark  : la session spark
    * @param url    : url complète du stockage
    * @param schema : le schéma des données à lire
    * @return Dataframe
    */
    def readData(spark: SparkSession, DATE: String, schema: StructType, nameEnv:String ): DataFrame = {
      val postURL = date2URL(DATE)
      if (Utils.envVar("TEST_MODE") == "False") {
        val URL = tableVar(nameEnv, "in_bucket")
        /* calcul du chemin pour lire les données sur minio */
        println("URL de lecture sur Minio : " + URL + postURL)

        var fromFolder = {
          println("Reading data from : " + URL + postURL)
          new Path(URL + postURL)
        }
        val conf = spark.sparkContext.hadoopConfiguration
        val logfiles = fromFolder.getFileSystem(conf)
          .listFiles(fromFolder, true)
        var files = Seq[String]()
        while (logfiles.hasNext) {
          // one can filter here some specific files
          files = files :+ logfiles.next().getPath().toString
        }
        //affiche le nombre de fichier trouvé
        println("number of files read : " + files.length)
        //affiche le path de tout les fichier trouvé
        println(files.mkString("-"))
        try {
          nameEnv match {
            case "tableExutoire" => {
              if (files.length == 1) {
                spark
                  .read
                  .format("com.crealytics.spark.excel")
                  .option("header", "true")
                  .option("inferSchema", "false")
                  .option("maxRowsInMemory", 1000)
                  .option("dataAddress", "'Données brutes Clear'!")
                  .load(URL + postURL +"/" + files(0).split("/").last)

              } else {
                throw new Exception("Trop de fichier excel a lire (seulement 1 doit être present)")
              }
            }
            case _ => {
              spark
                .read
                .option("header", Utils.tableVar(nameEnv, "header"))
                .option("compression", "gzip")
                .format(Utils.tableVar(nameEnv, "format"))
                .option("delimiter", Utils.tableVar(nameEnv, "delimiter"))
                .option("encoding", Utils.tableVar(nameEnv, "encoding"))
                .csv(URL + postURL)
            }
          }
        } catch {
          case e: Throwable =>
            println("ERROR while reading data at : " + URL + postURL + " \nError stacktrace :" + e)
            spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
        }

        // else... si nous somme en mode TEST
      }
      else {
        val URL = tableVar(nameEnv, "test_bucket")
        println("Chemin de lecture en local : " + URL + postURL)
        var fromFolder = {
          println("Reading data from : " + URL + postURL)
          new Path(URL + postURL)
        }
        val conf = spark.sparkContext.hadoopConfiguration
        val logfiles = fromFolder.getFileSystem(conf)
          .listFiles(fromFolder, true)
        var files = Seq[String]()
        while (logfiles.hasNext) {
          // one can filter here some specific files
          files = files :+ logfiles.next().getPath().toString
        }
        //affiche le nombre de fichier trouvé
        println("number of files read : " + files.length)
        //affiche le path de tout les fichier trouvé
        println(files.mkString("-"))
        nameEnv match {
          case "tableExutoire" => {
            if (DATE == "WrongDate") {
              spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
            } else {
              if (files.length == 1 && files(0).split("/").last.contains(".xlsx")) {
                spark
                  .read
                  .format("com.crealytics.spark.excel")
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .option("dataAddress", "0!")
                  .load(URL + postURL  + files(0).split("/").last)
              } else {
                spark
                  .read
                  .option("header", "true")
                  .format("csv")
                  .option("delimiter", ";")
                  .load(URL + postURL)
              }
            }
          }
          case _ => {
            if (DATE == "WrongDate") {
              spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
            } else {
              spark
                .read
                .option("header", "true")
                .option("compression", "gzip")
                .format("csv")
                .option("delimiter", ";")
                .load(URL + postURL)

            }
          }
        }
      }
    }

    /**
    * Permet de prendre une date en input et de ressortir la version partitionne
    *
    * @param DATE date sous forme  yyyy-mm-dd
    * @return string sous la forme yyyy/mm/ss ou ss est le numero de la semaine du mois courant
    */
    def date2URL(DATE: String): String = {
      if(DATE !="WrongDate"){
         val date = DATE.split("-") // donne la date sous forme yyyy-mm-dd
        val year = date(0);
        val month = date(1);
        val day = date(2);
        val postURL = "year=" + year + "/month=" + month + "/day=" + day + "/";
        return postURL
      }else {
        return ""
      }
     
    }

  def getMap(spark: SparkSession, nameEnv:String): (Map[String,String],Map[String,(String,String)]) ={
    var mapName:Map[String,String] = Map()
    var mapParam:Map[String,(String,String)] = Map()
    var i=0
    val iterator = Utils.getIterator(nameEnv,"fields")
    while(iterator.hasNext){
      val current = iterator.next
      val name = current.getString("name")
      mapName+= ("_c"+i -> name)
      mapParam +=(name -> (current.getString("type"),current.getString("nullable")))
      i=i+1
    }
    return (mapName,mapParam)
  }

  def getSchema(nameEnv:String): StructType={
    val iterator = Utils.getIterator(nameEnv,"fields")
      var map = new StructType()
      while(iterator.hasNext){
        val current = iterator.next
        val datatype =  current.getString("type") match {
          case "StringType" => StringType
          case "DoubleType" => DoubleType
          case "BooleanType" => BooleanType
          case "IntegerType" => IntegerType
          case _ => StringType
        }
        map = map.add(current.getString("name"), datatype, current.getString("nullable").toBoolean)
      }
    map
  }

  def getIterator(nameEnv:String, name: String):Iterator[Config] = {
      val listFields = config.getConfigList(nameEnv + "." +name) 
      listFields.iterator()
  }

    def getListName(nameEnv:String): Seq[String]={
    val iterator = Utils.getIterator(nameEnv,"fields")
      var list = List[String]()
      while(iterator.hasNext){
        val current = iterator.next
        list =list :+ current.getString("name")
      }
    list
  }

  def renommageColonnes(spark: SparkSession, df_toRename: DataFrame, mapName:Map[String,String] ): DataFrame ={
    var df_Rename = mapName.foldLeft(df_toRename){
      case (df_toRename, (oldName, newName)) => df_toRename.withColumnRenamed(oldName, newName)
    }
    df_Rename
  }

  def castDF(spark: SparkSession, df_toCast: DataFrame,nameEnv:String, mapNametoParam:Map[String,(String, String)]): DataFrame ={
    var df_Cast = mapNametoParam.foldLeft(df_toCast){
      case (df_toCast, (columnName, param)) => param._1 match {
        case "DoubleType" => 
          val toDouble = udf((s:String) => UDF.udfToDouble(s))
          df_toCast.withColumn(columnName,toDouble(col(columnName)).cast(DoubleType)) 
        case "IntegerType" =>
          val toInteger = udf((s:String) =>UDF.udfToInt(s))
          df_toCast.withColumn(columnName,toInteger(col(columnName)).cast(IntegerType))
        case "BooleanType" => 
          val toBoolean = udf((s:String) =>UDF.udfToBoolean(s))
          df_toCast.withColumn(columnName,toBoolean(col(columnName)).cast(BooleanType))
        case "FloatType" => 
          val toFloat = udf((s:String) =>UDF.udfToFloat(s))
          df_toCast.withColumn(columnName,toFloat(col(columnName)).cast(FloatType))
        case _ => 
          val toString = udf((s:String) =>UDF.udfToString(s))
          df_toCast.withColumn(columnName,toString(col(columnName)))
      }
    }

    df_Cast
  }

  def sort(s1:String, s2:String): Boolean ={
    try{
      s1.replace("_c","").toInt < s2.replace("_c","").toInt
    }catch{
      case e:Throwable => print("ERROR dans le sort avec les données s1:"+s1+" et s2:"+ s2+"\n"+e)
      return s1<s2
    }
   
  }

  def writeToS3(spark: SparkSession, df_toWrite: DataFrame,nameEnv:String,csv: String): Unit = {
    writeToS3(spark,df_toWrite,nameEnv,csv, java.time.LocalDate.now.toString)
     
  }

  def writeToS3(spark: SparkSession, df_toWrite: DataFrame,nameEnv:String,csv: String, DATE: String): Unit = {
    val postURL = date2URL(DATE)
    println("Write to s3 to " + Utils.tableVar(nameEnv,"out_bucket") + postURL)

    df_toWrite   // nécessaire pour écrire le DF dans un seul csv, mais pPeut poser problème si le DF est trop gros
      .write.options(Map("header"->"true", "delimiter"->";"))
      .mode(SaveMode.Append)
      .orc(Utils.tableVar(nameEnv,"out_bucket") + postURL)
    if(csv!="false"){
      df_toWrite   // nécessaire pour écrire le DF dans un seul csv, mais pPeut poser problème si le DF est trop gros
        .write.options(Map("header"->"true", "delimiter"->";"))
        .mode(SaveMode.Append)
        .csv(Utils.tableVar(nameEnv,"out_bucket") + postURL)
       println("Write to s3 Done")
    }

  }

  def regexCharSpe(spark: SparkSession, df_toRename: DataFrame): DataFrame ={
    val columns = df_toRename.columns //.toString().toUpperCase()
    val regexAlphabet = """[^\w A-Za-z]""" //Search all characters (except alphabet latin)
    val regexSpace = """[\n# $&:\n\t]"""  //Search for white spaces

    val normalizedColumns = columns.map(StringUtils.stripAccents)//remplacer les caractères accentués en non-accentués
    val resultDFnormalized = normalizedColumns.zip(columns).foldLeft(df_toRename){(tempdf, name) => tempdf.withColumnRenamed(name._2, name._1)}

    val charspeColumns = normalizedColumns.map(regexAlphabet.r.replaceAllIn(_, "")) //remplacer par vide les characteres speciaux
    val resultDF = charspeColumns.zip(normalizedColumns).foldLeft(resultDFnormalized){(tempdf, name) => tempdf.withColumnRenamed(name._2, name._1)}

    val spaceColumns = charspeColumns.map(regexSpace.r.replaceAllIn(_, "_")) //remplacer par underscore les espaces
    val resultDF2 = spaceColumns.zip(charspeColumns).foldLeft(resultDF){(tempdf, name) => tempdf.withColumnRenamed(name._2.toUpperCase(), name._1)}
    resultDF2
  }

  def lowerCaseAllHeader(spark: SparkSession, df_toRename: DataFrame): DataFrame ={
    val columns = df_toRename.columns //.toString().toUpperCase()

    val loweredColumns = columns.map(StringUtils.lowerCase)//remplacer par des charactèes en minuscule
    val resultDFLowered = loweredColumns.zip(columns).foldLeft(df_toRename){(tempdf, name) => tempdf.withColumnRenamed(name._2, name._1)}

    resultDFLowered
  }

}
