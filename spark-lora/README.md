# Build project

```bash
    mvn clean install
```

## spark-submit

```bash
Lancement du container Spark :
    ./dc.sh exec spark bash

Commande en local :
spark-submit --class fr.rennesmetropole.app.ExecuteLoraAnalysis  --files /app-lora/conf/application.conf --conf "spark.driver.extraJavaOptions=-Dconfig.file=/app-lora/conf/application.conf" --conf "spark.executor.extraJavaOptions=-Dconfig.file=/app-lora/conf/application.conf" /app-lora/lora/target/rm-lora-analysis-1.0-SNAPSHOT.jar 2021-09-27 
    

```

## PostgresSQL

**Mise a jour des tables :**
Pour changer les tables postgres en local, on peut le faire soit manuellement directement depuis pgAdmin via la console, ou alors : 
 - changer manuellement le script compose/analysis-db/0_init.sh avec les nouvelles valeur (nom de table, nouvelle table, nouvelle colonne, renommage de colonne, etc) puis lancer le script depuis le container analysis_db :
    - une fois dans le container faire :

    ```bash
        cd docker-entrypoint-initdb.d/
    ```
    puis :
    ```bash
        ./0_init.sh
    ```
**Commande utile pour visionner les données**
- Commande permettant de voir combien de données/lignes on a pour chaque capteur :

```SQL
SELECT "deveui",COUNT(*) FROM dwh.lora_energy_data GROUP BY "deveui" ORDER BY "deveui" desc;
```

- Commande permettant de voir toutes les données trié par deveui :

```SQL
SELECT * FROM dwh.lora_energy_data ORDER BY "deveui" desc;
```
- Commande permettant de voir toutes pour un deveui précis :

```SQL
SELECT * FROM dwh.lora_energy_data WHERE "deveui" = 'mondeveui' ORDER BY "deveui" desc;
```

```SQL
SELECT deveui,enabledon,tramedate,name,value,unitid,measurenatureid,inserteddate FROM dwh.lora_energy_data WHERE tramedate >= '2021-11-02';
```



## Traitement
Le fichier Utils.Traitements regroupe tout les traitements correspondant aux capteurs.
Point important, chaque dataframe qui sort d'un traitement doit avoir le schéma suivant 

| deveui | timestamp | name | value | insertedDate | id
| ------ | ---- | ---- | ---- | ---- | ---- |
| -- | -- | -- || -- | -- | -- |