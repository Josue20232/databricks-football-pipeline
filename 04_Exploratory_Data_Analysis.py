# Databricks notebook source
# ÉTAPE 1 : Import des fonctions PySpark nécessaires
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# COMMAND ----------

 # Load the league ranking data from the Gold table 
df_football_ranking = spark.read.table("football_matches_catalog.gold.football_matches_ranking") 
display(df_football_ranking)  

# COMMAND ----------

#Step 1: filter the teams
df_champions_by_season = (
    df_football_ranking
    .filter(col("TeamPosition")==1)
    .drop("TeamPosition")
    .orderBy("Season")
)

#Step 2: display the results
display(df_champions_by_season)

# COMMAND ----------

# ÉTAPE 4 : le nombre de titres par équipe

# Créer le DataFrame des titres par équipe
df_titles_by_team = (
    df_football_ranking
    .filter(col("TeamPosition") == 1)  # Garder seulement les champions
    .groupBy("Team")  # Grouper par équipe
    .agg(
        F.count("*").alias("Titles")  # Compter le nombre de titres
    )
    .orderBy(F.desc("Titles"), "Team")  # Trier par nombre de titres (décroissant), puis par nom d'équipe
)

# Afficher les résultats
print(f"{df_titles_by_team.count()} équipes ont remporté au moins un titre")
display(df_titles_by_team)

# COMMAND ----------

# ÉTAPE 5 : Meilleure attaque par saison

# la fenêtre pour trouver la meilleure attaque
window_spec = Window.partitionBy("Season").orderBy(F.desc("GoalsScored"))

# le DataFrame de la meilleure attaque par saison
df_best_attack_by_season = (
    df_football_ranking
    .withColumn("rank", F.row_number().over(window_spec))  # un rang par saison
    .filter(col("rank") == 1)  # seulement le premier rang (plus de buts)
    .select("Season", "Team", "GoalsScored")  # les colonnes demandées
    .orderBy("Season")  # par saison
)

# les résultats
print(f"Meilleures attaques identifiées pour {df_best_attack_by_season.count()} saisons")
display(df_best_attack_by_season)

# COMMAND ----------

# ÉTAPE 6 : Vérification et sauvegarde
# 1. Champions par saison
print("1. Champions par saison :")
print(f"   • Nombre de champions : {df_champions_by_season.count()}")
print(f"   • Première saison : {df_champions_by_season.select(F.min('Season')).collect()[0][0]}")
print(f"   • Dernière saison : {df_champions_by_season.select(F.max('Season')).collect()[0][0]}")

# 2. Titres par équipe
print("\n2. Titres par équipe :")
top_team = df_titles_by_team.orderBy(F.desc("Titles")).first()
print(f"   • Équipe la plus titrée : {top_team['Team']} ({top_team['Titles']} titres)")
print(f"   • Nombre d'équipes avec titres : {df_titles_by_team.count()}")

# 3. Meilleure attaque
print("\n3. Meilleure attaque par saison :")
avg_goals = df_best_attack_by_season.select(F.round(F.avg("GoalsScored"), 2)).collect()[0][0]
print(f"   • Moyenne de buts de la meilleure attaque : {avg_goals} buts/saison")

# Optionnel : Sauvegarder les résultats dans des tables séparées
print("\nSauvegarde des résultats")
try:
    df_champions_by_season.write.format("delta").mode("overwrite").saveAsTable("football_matches_catalog.gold.champions")
    print("Champions sauvegardés")
except:
    print("Impossible de sauvegarder les champions")

try:
    df_titles_by_team.write.format("delta").mode("overwrite").saveAsTable("football_matches_catalog.gold.titles")
    print("Titres par équipe sauvegardés")
except:
    print("Impossible de sauvegarder les titres")

try:
    df_best_attack_by_season.write.format("delta").mode("overwrite").saveAsTable("football_matches_catalog.gold.best_attacks")
    print("Meilleures attaques sauvegardées")
except:
    print("Impossible de sauvegarder les meilleures attaques")


# COMMAND ----------

