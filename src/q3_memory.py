# Databricks notebook source
# MAGIC %run /src/utils/functions

# COMMAND ----------

#mentionedUsers
from typing import List, Tuple
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql.functions import col, to_date, lit, count, row_number, substring, explode, trim
from pyspark.storagelevel import StorageLevel

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    #Creamos sesión de spark:
    spark = create_spark_session()

    #Leemos el .json
    df_json = read_json(spark, file_path)

    #Realizamos las tranformaciones necesarias
    ##Paso1: Quedarnos solo con los campos necesarios (mentionedUsers.username)
    df_universo = df_json.select(
                            col("mentionedUsers.username").alias("mentionedUsers")
    )
    #df_universo.display()
    #print(df_universo.columns)
    ##Paso2: Nos quedamos con los no nulos
    df_universo_01 = df_universo.where(col('mentionedUsers').isNotNull())
    ##Paso3: Pasamos a tener los usuarios en una columna y no en una lista por fila
    df_mentioned_users = df_universo_01.select(explode(col('mentionedUsers')).alias('mentionedUsers'))
    #df_mentioned_users.display()
    
    #print(df_universo_01.count())
    #print(df_mentioned_users.count())

    ##Paso4: Una vez tenemos a los usuarios, vamos a agruparlos, haciendo un trim por si es que hay algun usuario con espacios a la derecha o a la izquierda
    df_unique_mentioned_users = df_mentioned_users.groupby(trim(col('mentionedUsers')).alias('mentionedUsers')).agg(count(lit(1)).alias('ctd_user_mentioned'))

    #df_unique_mentioned_users.display()
    df_desc_unique_mentioned_users = df_unique_mentioned_users.orderBy('ctd_user_mentioned', ascending=False)

    #Almacenamos el resultado en el formato pedido
    result = write_to_tuples(df_desc_unique_mentioned_users, "mentionedUsers", "ctd_user_mentioned")
    #print(result)
    return result

q3_memory("dbfs:/FileStore/shared_uploads/taboada.jose@pucp.pe/farmers_protest_tweets_2021_2_4.json")
