# Spark data-processing layer

## spark-submit

```bash

    mvn package -DskipTests=true
    ./dc.sh exec spark bash
    #spark-submit --class App /app2/target/spark_example-1.0-SNAPSHOT.jar
    commande en prod :
    spark-submit --class fr.rennesmetropole.app.ExecuteTrafficAnalysis --master spark://spark:7077 --executor-memory 8G --total-executor-cores 8 --driver-memory 12G /app/target/rm-traffic-analysis-1.0.0.jar 2021-07-07 15

    commande en local :
   spark-submit --driver-memory=7G --executor-memory=6G --total-executor-cores 12 --executor-cores 6 --master spark://spark:7077 --class fr.rennesmetropole.app.ExecuteTrafficAnalysis /app/target/rm-traffic-analysis-2.3.0-SNAPSHOT.jar 2021-07-07 15 


```

## spark-shell

Work-in-progress / Worknotes

```bash
    ./dc.sh exec spark bash
    cd /app/

    spark-shell --driver-java-options="-Dhttp.proxyHost=proxy -Dhttp.proxyPort=80" --conf spark.jars.ivy=/opt/bitnami/spark/ivy --packages io.minio:spark-select_2.11:2.1

    spark-shell --conf spark.jars.ivy=/opt/bitnami/spark/ivy --packages io.minio:spark-select_2.11:2.1
    
    :load ReadSelectTest.scala
    App.main(Array())
```

```powershell
    $Env:JAVA_HOME="C:\Portable\java-se-8u41-ri"
    $Env:MAVEN_OPTS="-Djavax.net.debug=all"
```

## Spark documentation essentials

- [Spark configuration] (https://spark.apache.org/docs/3.0.1/configuration.html)
- [Spark Scala API](https://spark.apache.org/docs/2.4.3/api/scala/index.html#org.apache.spark.sql.Dataset)
- [MinIO Spark Select](https://github.com/minio/spark-select)

## PostgresSQL configuration
 - Set of primary key : 
 ```
ALTER TABLE IF EXISTS ONLY traffic_analysis
ADD CONSTRAINT pk_trafficAnalysis PRIMARY KEY('A RENSEIGNER');
```

## Ecriture sur Postgres
Il faut savoir que l'écriture en mode Overwrite drop la table pour y mettre le dataFrame donné en paramètre.
La solution actuelle est pertinente que pour des donneés partitionné. Elle va supprimé les données de la semaine courante ( chaque éxecution) et y insérer les données de toutes la semaine.

## Clé Primaire sur la table traffic_analysis
La colonne 'technical_key' est la concaténation de la colonne start_time, id et des 3 colonnes de partionnements.
Cette colonne est née en réponse au problème de doublons avec l'ancienne clé primaire qui était sur id et start_time. Si l'on effectuait une aggrégation sur 2 semaine différentes contenant le même quart d'heure (quart d'heure incomplet car coupé et a cheval sur 2 semaines) le job tombait une erreur pour cause de doublon. 
La colonne 'technical_key' permet donc de mettre en place une clé primaire (pour eviter d'inséré des doublons) tout en evitant de faire échouer le job dans ce cas précis.

## Accessibilité
 Spark est accessible via l'url Spark Master at spark://9a88bccdd39b:7077 (rennesmetropole.fr) avec la même authentification que lora
 