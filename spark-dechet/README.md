# Build project

```bash
    mvn clean install
```

## spark-submit

```bash
Lancement du container Spark :
    ./dc.sh exec spark bash
```
## History event 
Executer les commande suivante pour activer l'historisation possible des jobs qui seront visible ensuite via http://spark(localhost):18080
```
mkdir /tmp/spark-events
start-history-server.sh
```
## Serveur FTP
Via FileZilla pour accéder au serveur FTP (Déchets IDEA) => Hôte: widata.simpliciti.fr | identifiant: rennes | mdp: *voir .env* | Port: 4021
### Commande Collecte
Commande pour préparé les données de collecte :
```
spark-submit --class fr.rennesmetropole.app.ExecuteDechetPreparation  --files /app-dechet/conf/application.conf --conf "spark.driver.extraJavaOptions=-Dconfig.file=/app-dechet/conf/application.conf" --conf "spark.executor.extraJavaOptions=-Dconfig.file=/app-dechet/conf/application.conf"  /app-dechet/dechet-preparation/target/rm-dechet-preparation-4.0-SNAPSHOT.jar 2022-01-07
```

Commande pour l'enrichissement des données déchets de collecte :
```
spark-submit --class fr.rennesmetropole.app.ExecuteDechetAnalysis  --files /app-dechet/conf/application.conf --conf "spark.driver.extraJavaOptions=-Dconfig.file=/app-dechet/conf/application.conf" --conf "spark.executor.extraJavaOptions=-Dconfig.file=/app-dechet/conf/application.conf"  /app-dechet/dechet-analysis/target/rm-dechet-analysis-4.0-SNAPSHOT.jar 2022-mm-dd
```


### Commande Référentiel
Commande pour préparé les données Référentiel :
```
spark-submit --class fr.rennesmetropole.app.ExecuteDechetRefPreparation  --files /app-dechet/conf/application.conf --conf "spark.driver.extraJavaOptions=-Dconfig.file=/app-dechet/conf/application.conf" --conf "spark.executor.extraJavaOptions=-Dconfig.file=/app-dechet/conf/application.conf"  /app-dechet/dechet-preparation/target/rm-dechet-preparation-4.0-SNAPSHOT.jar 2021-12-23
```

Commande pour l'enrichissement des données déchets Référentiel :
```
  spark-submit --master spark://spark:7077  --executor-cores 8 --executor-memory 12G --driver-memory 8G --class fr.rennesmetropole.app.ExecuteDechetRefAnalysis --files /app-dechet/conf/application.conf --conf "spark.driver.extraJavaOptions=-Dconfig.file=/app-dechet/conf/application.conf" --conf "spark.executor.extraJavaOptions=-Dconfig.file=/app-dechet/conf/application.conf"  /app-dechet/dechet-analysis/target/rm-dechet-analysis-4.0-SNAPSHOT.jar 2022-04-15
```

### Commande Exutoire
Commande pour préparé les données Exutoire :
```
spark-submit --class fr.rennesmetropole.app.ExecuteDechetExutoirePreparation  --files /app-dechet/conf/application.conf --conf "spark.driver.extraJavaOptions=-Dconfig.file=/app-dechet/conf/application.conf" --conf "spark.executor.extraJavaOptions=-Dconfig.file=/app-dechet/conf/application.conf"  /app-dechet/dechet-preparation/target/rm-dechet-preparation-4.0-SNAPSHOT.jar 2022-02-17
```
