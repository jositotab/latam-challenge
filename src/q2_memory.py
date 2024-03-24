# Databricks notebook source
#%pip install emoji

# COMMAND ----------

# MAGIC %run /src/utils/functions

# COMMAND ----------

from typing import List, Tuple
import re
import emoji
from pyspark.sql.functions import col, to_date, lit, count, row_number, substring, explode, split, udf
from pyspark.sql.types import BooleanType
from pyspark.sql import Window
from memory_profiler import profile as mem_profile
from line_profiler import profile as line_profile
    
def es_emoji(caracter):
    #El formato unicode de los emojis es \uXXXX
    # Patrón de expresión regular para encontrar emojis Unicode
    return emoji.is_emoji(caracter)


#@mem_profile
#@line_profile
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    #Inicializamos spark
    spark = create_spark_session()
    #Lectura del json
    df_json = read_json(spark, file_path)
    #Obtenemos el campo a analizar
    df_universo = df_json.select("content")


    
    print(es_emoji("\u2764\ufe0f"))
    #Registramos la función como UDF en spark
    es_emoji_udf = udf(es_emoji, BooleanType())

    df_emojis = df_universo.select(explode(split(col('content'), '')).alias('caracteres')).filter(es_emoji_udf(col('caracteres')))

    df_emojis_3 = df_emojis.groupby('caracteres').agg(count(lit(1)).alias("ctd_emoji")).orderBy("ctd_emoji", ascending=False)
    #window_spec = Window.partitionBy("caracteres")
    #df_emojis_3 = df_emojis_2.select(col('caracteres'), 
    #                                 col('ctd_emoji'), 
    #                                 row_number().over(orderBy(col("ctd_emoji").desc())).alias("top_n"))
    #df_emojis_3.display()

    #df_emojis_4 = df_emojis_3.where(col('top_n') <= 10)

    #df_emojis_4.display()

    #Almacenamos el resultado en el formato pedido
    result = write_to_tuples(df_emojis_3, "caracteres", "ctd_emoji")
    #print(result)

    return result
    
#q2_memory("dbfs:/FileStore/shared_uploads/taboada.jose@pucp.pe/farmers_protest_tweets_2021_2_4.json")