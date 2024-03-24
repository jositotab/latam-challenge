# Databricks notebook source
# MAGIC %pip install emoji

# COMMAND ----------

# MAGIC %run /src/utils/functions

# COMMAND ----------

# MAGIC %run /src/q1_time

# COMMAND ----------

# MAGIC %run /src/q1_memory

# COMMAND ----------

# MAGIC %run /src/q2_time

# COMMAND ----------

# MAGIC %run /src/q2_memory

# COMMAND ----------

# MAGIC %run /src/q3_time

# COMMAND ----------

# MAGIC %run /src/q3_memory

# COMMAND ----------

json_path = "dbfs:/FileStore/shared_uploads/taboada.jose@pucp.pe/farmers_protest_tweets_2021_2_4.json"
q1_time(json_path)

# COMMAND ----------

q1_memory(json_path)

# COMMAND ----------

