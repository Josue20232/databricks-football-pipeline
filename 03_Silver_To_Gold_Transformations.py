# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_silver_football_matches = spark.read.table("football_matches_catalog.silver.football_matches")

# COMMAND ----------


display(df_silver_football_matches)

# COMMAND ----------

#filter rows
df_filtered = df_silver_football_matches.filter(
    (col("HomeTeamGoals")>= 0) &
    (col("AwayTeamGoals")>= 0)
)


# COMMAND ----------

df_home_team_stats = (
    df_filtered
    .withColumnRenamed("HomeTeam", "Team")
    .groupBy("Season", "Team")
    .agg(
        sum("HomeTeamGoals").alias("GoalsScored"),
        sum("AwayTeamGoals").alias("GoalsAgainst"),
        sum(when(col("FinalResult") == "H", 1).otherwise(0)).alias("Win"),
        sum(when(col("FinalResult") == "A", 1).otherwise(0)).alias("Loss"),
        sum(when(col("FinalResult") == "D", 1).otherwise(0)).alias("Tie")
    )
)


display(df_home_team_stats)

# COMMAND ----------

df_away_team_stats = (
    df_filtered
    .withColumnRenamed("AwayTeam", "Team")
    .groupBy("Season", "Team")
    .agg(
        sum("AwayTeamGoals").alias("GoalsScored"),
        sum("HomeTeamGoals").alias("GoalsAgainst"),
        sum(when(col("FinalResult") == "A", 1).otherwise(0)).alias("Win"),
        sum(when(col("FinalResult") == "H", 1).otherwise(0)).alias("Loss"),
        sum(when(col("FinalResult") == "D", 1).otherwise(0)).alias("Tie")
    )
)


display(df_away_team_stats)

# COMMAND ----------

df_season_team_agg_stats = (
    df_home_team_stats.union(df_away_team_stats)
    .groupBy("Season", "Team")
    .agg(
        sum("GoalsScored").alias("GoalsScored"),
        sum("GoalsAgainst").alias("GoalsAgainst"),
        sum("Win").alias("Win"),
        sum("Loss").alias("Loss"),
        sum("Tie").alias("Tie")
    )
)

display(df_season_team_agg_stats)

# COMMAND ----------

from pyspark.sql.functions import round

df_season_team_enriched_stats = df_season_team_agg_stats.withColumn(
    "GoalDifferentials", col("GoalsScored") - col("GoalsAgainst")
).withColumn(
    "WinPercentage", 
    round(col("Win") / (col("Win") + col("Loss") + col("Tie")) * 100, 2)
)


display(df_season_team_enriched_stats)

# COMMAND ----------

from pyspark.sql.functions import round, col 
#Add Goal Differentials and WinPercentage columns
df_season_team_enriched_stats = (
     df_season_team_agg_stats
     .withColumn( "GoalDifferentials", col("GoalsScored") - col("GoalsAgainst") )
     .withColumn( "WinPercentage", round(col("Win") / (col("Win") + col("Loss") + col("Tie")) * 100, 2) )
)

display(df_season_team_enriched_stats)

# COMMAND ----------

from pyspark.sql.window import Window


window = Window.partitionBy("Season").orderBy(col("Win").desc(), col("GoalDifferentials").desc())


df_gold_team_rankings = df_season_team_enriched_stats.withColumn(
    "TeamPosition", row_number().over(window)
).orderBy("Season", "TeamPosition")


display(df_gold_team_rankings)

# COMMAND ----------

# ÉTAPE 7 : Sauvegarde dans la couche Gold

df_gold_team_rankings.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("football_matches_catalog.gold.football_matches_ranking")

# COMMAND ----------

