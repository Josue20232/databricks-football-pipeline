# Databricks notebook source
from pyspark.sql.functions import * 

# COMMAND ----------

# Read data from the Bronze table 
df_silver = spark.read.table("football_matches_catalog.bronze.football_matches")

# COMMAND ----------

 display(df_silver) 


# COMMAND ----------

from pyspark.sql.functions import col, to_date
# 1. Filtrer uniquement les matchs où Div = 'D1'
df_silver = df_silver.filter(col("Div") == "D1")

print(" Filtrage D1 terminé")

# 2. Convertir Match_ID, FTHG, FTAG en integer
df_silver = df_silver \
    .withColumn("Match_ID", col("Match_ID").cast("integer")) \
    .withColumn("FTHG", col("FTHG").cast("integer")) \
    .withColumn("FTAG", col("FTAG").cast("integer"))

print(" Conversion des types terminée")

# 3. Convertir la colonne Date en type date (format yyyy-MM-dd)
df_silver = df_silver.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

print(" Conversion de Date terminée")

# 4. Renommer les colonnes
df_silver = df_silver \
    .withColumnRenamed("FTHG", "HomeTeamGoals") \
    .withColumnRenamed("FTAG", "AwayTeamGoals") \
    .withColumnRenamed("FTR", "FinalResult")

print(" Renommage des colonnes terminé")

# Afficher le résultat
print(" Résultat final des transformations :")
display(df_silver)

# COMMAND ----------

# Save the DataFrame as a Delta table in the Silver layer, partitioned by Season
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteschema", "true") \
    .partitionBy("Season") \
    .saveAsTable("football_matches_catalog.silver.football_matches")

# COMMAND ----------

