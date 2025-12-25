# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC import os
# MAGIC import sys
# MAGIC project_path = os.path.join(os.getcwd(),'../..')
# MAGIC sys.path.append(project_path)
# MAGIC from utils.transformations import Reusable
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # **DimUser**

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format","parquet")\
                .option("cloudFiles.schemaLocation","abfss://silver@mukundadatalake.dfs.core.windows.net/DimUser/checkpoint")\
                .load("abfss://bronze@mukundadatalake.dfs.core.windows.net/DimUser")

# COMMAND ----------

df_user = df_user.withColumn("user_name", upper(col("user_name")))

# COMMAND ----------


reusable_obj = Reusable()
df_user = reusable_obj.dropColumns(df_user,['_rescued_data'])
df_user = df_user.dropDuplicates(['user_id'])

# COMMAND ----------

df_user.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation","abfss://silver@mukundadatalake.dfs.core.windows.net/DimUser/checkpoint")\
        .option("path","abfss://silver@mukundadatalake.dfs.core.windows.net/DimUser/data")\
        .trigger(once = True)\
        .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC # **DimArtist**

# COMMAND ----------

df_artist = spark.readStream.format("cloudFiles")\
                 .option("cloudFiles.format","parquet")\
                 .option("cloudFiles.schemaLocation","abfss://silver@mukundadatalake.dfs.core.windows.net/DimArtist/checkpointAL")\
                 .load("abfss://bronze@mukundadatalake.dfs.core.windows.net/DimArtist")

# COMMAND ----------

reusable_obj = Reusable()
df_artist = reusable_obj.dropColumns(df_artist,['_rescued_data'])
df_artist = df_artist.dropDuplicates(['artist_id'])

# COMMAND ----------

df_artist.writeStream.format("delta")\
         .outputMode("append")\
         .option("checkpointLocation","abfss://silver@mukundadatalake.dfs.core.windows.net/DimArtist/checkpoint")\
         .option("path","abfss://silver@mukundadatalake.dfs.core.windows.net/DimArtist/data")\
         .trigger(once = True)\
         .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC # **DimTrack**

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format","parquet")\
                .option("cloudFiles.schemaLocation","abfss://silver@mukundadatalake.dfs.core.windows.net/DimTrack/checkpointAL")\
                .load("abfss://bronze@mukundadatalake.dfs.core.windows.net/DimTrack")

# COMMAND ----------

reusable_obj = Reusable()
df_track = reusable_obj.dropColumns(df_track,['_rescued_data'])
df_track = df_track.withColumn("durationFlag",when(col("duration_sec") <150,"low")\
                                             .when(col("duration_sec") <300 ,"medium")\
                                             .otherwise("high"))

df_track = df_track.withColumn("track_name",regexp_replace(col("track_name"),'-',' '))

# COMMAND ----------

df_track.writeStream.format("delta")\
         .outputMode("append")\
         .option("checkpointLocation","abfss://silver@mukundadatalake.dfs.core.windows.net/DimTrack/checkpoint")\
         .option("path","abfss://silver@mukundadatalake.dfs.core.windows.net/DimTrack/data")\
         .trigger(once = True)\
         .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC # **DimDate**

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format","parquet")\
                .option("cloudFiles.schemaLocation","abfss://silver@mukundadatalake.dfs.core.windows.net/DimDate/checkpointAL")\
                .load("abfss://bronze@mukundadatalake.dfs.core.windows.net/DimDate")

# COMMAND ----------

reusable_obj = Reusable()
df_date = reusable_obj.dropColumns(df_date,['_rescued_data'])

# COMMAND ----------

df_date.writeStream.format("delta")\
         .outputMode("append")\
         .option("checkpointLocation","abfss://silver@mukundadatalake.dfs.core.windows.net/DimDate/checkpoint")\
         .option("path","abfss://silver@mukundadatalake.dfs.core.windows.net/DimDate/data")\
         .trigger(once = True)\
         .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC # **FactStream**

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format","parquet")\
                .option("cloudFiles.schemaLocation","abfss://silver@mukundadatalake.dfs.core.windows.net/FactStream/checkpointAL")\
                .load("abfss://bronze@mukundadatalake.dfs.core.windows.net/FactStream")

# COMMAND ----------

reusable_obj = Reusable()
df_fact = reusable_obj.dropColumns(df_fact,['_rescued_data'])

# COMMAND ----------

df_fact.writeStream.format("delta")\
         .outputMode("append")\
         .option("checkpointLocation","abfss://silver@mukundadatalake.dfs.core.windows.net/FactStream/checkpoint")\
         .option("path","abfss://silver@mukundadatalake.dfs.core.windows.net/FactStream/data")\
         .trigger(once = True)\
         .toTable("spotify_cata.silver.FactStream")