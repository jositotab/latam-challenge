# Databricks notebook source
# MAGIC %md
# MAGIC ***LATAM CHALLENGUE***
# MAGIC El objetivo de este challengue es definir 6 funciones en base a 3 ejercicios con enfoque en tiempo de ejecución y memoria.
# MAGIC
# MAGIC --En los casos donde aplicaba, se uso persist(DISK_ONLY) para reducir el uso de RAM, y cache() para reducir tiempos de ejeución almacenando el dataframe en memoria caché. Por otro lado, se definió la estructura que esperabamos del .json en los scripts de tiempo de ejecucición para que no se ponga a inferir el schema y trabaje solo con los campos necesarios.
# MAGIC
# MAGIC Los ejericicios son los siguientes:
# MAGIC
# MAGIC   **Ejercicio 1: Las top 10 fechas donde hay más tweets. Mencionar el usuario (username) que más publicaciones tiene por cada uno de esos días**
# MAGIC
# MAGIC   **Ejercicio 2: Los top 10 emojis más usados con su respectivo conteo**
# MAGIC
# MAGIC   **Ejercicio 3: El top 10 histórico de usuarios (username) más influyentes en función del conteo de las menciones (@) que registra cada uno de ellos**

# COMMAND ----------

# MAGIC %md
# MAGIC Se importan las funciones y se instalan las dependencias necesarias

# COMMAND ----------

# MAGIC %pip install memory-profiler line-profiler emoji

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

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **SETEO DE PATH PARA TODOS LAS FUNCIONES**

# COMMAND ----------

json_path = "dbfs:/FileStore/shared_uploads/taboada.jose@pucp.pe/farmers_protest_tweets_2021_2_4.json"

# COMMAND ----------

# MAGIC %md
# MAGIC **EJERCICIO 1: Las top 10 fechas donde hay más tweets. Mencionar el usuario (username) que más publicaciones tiene por cada uno de esos días**
# MAGIC

# COMMAND ----------

q1_time(json_path)

# COMMAND ----------

q1_memory(json_path)

# COMMAND ----------

q2_time(json_path)

# COMMAND ----------

q2_memory(json_path)

# COMMAND ----------

q3_time(json_path)

# COMMAND ----------

q3_memory(json_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Para poder realizar el POST se pondrán los siguientes comandos:
# MAGIC
# MAGIC

# COMMAND ----------

import requests
import json

url = 'https://advana-challenge-check-api-cr-k4hdbggvoq-uc.a.run.app/data-engineer'

data = {
    "name": "Jose Taboada",
    "mail": "taboada.jose@pucp.pe",
    "github_url": "https://github.com/jositotab/latam-challenge.git"
}

response = requests.post(url, json=data)

print(response.text)