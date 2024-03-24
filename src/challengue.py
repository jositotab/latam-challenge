# Databricks notebook source
# MAGIC %run /src/q1_time
# MAGIC %run /src/q1_memory
# MAGIC %run /src/q2_time
# MAGIC #%run /src/q2_memory
# MAGIC #%run /src/q3_time
# MAGIC #%run /src/q3_memory
# MAGIC
# MAGIC json_path = "dbfs:/FileStore/shared_uploads/taboada.jose@pucp.pe/farmers_protest_tweets_2021_2_4.json"
# MAGIC

# COMMAND ----------

q1_time(json_path)

# COMMAND ----------

q1_memory(json_path)

# COMMAND ----------

