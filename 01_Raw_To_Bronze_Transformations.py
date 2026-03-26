# Databricks notebook source
# Define file path to the raw CSV file 
raw_path ="/Volumes/football_matches_catalog/raw/raw_files/football_matches_2016.csv" 
# Read raw CSV file 
df_football_matches = spark.read.format("csv") \
         .option("header", True) \
         .load(raw_path) 

# COMMAND ----------

# Define file path to the raw CSV file 
raw_path ="/Volumes/football_matches_catalog/raw/raw_files/*.csv" 
# Read raw CSV file 
df_football_matches = spark.read.format("csv") \
         .option("header", True) \
         .load(raw_path) 

# COMMAND ----------

display(df_football_matches)

# COMMAND ----------

# Path to the bronze layer table 
bronze_path = "football_matches_catalog.bronze.football_matches" 
# Save the DataFrame as a Delta table in the bronze layer 
(df_football_matches.write.format("delta") 
    .mode("overwrite") 
    .saveAsTable(bronze_path)) 

# COMMAND ----------

