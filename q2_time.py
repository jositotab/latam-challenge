# Databricks notebook source
# MAGIC %pip install emoji

# COMMAND ----------

# MAGIC %run /src/utils/functions

# COMMAND ----------

from typing import List, Tuple
import re
import emoji
from pyspark.sql.functions import col, to_date, lit, count, row_number, substring, explode, split, udf
from pyspark.sql.types import BooleanType, StructType, StringType, StructField
from pyspark.sql import Window


    
def es_emoji(caracter):
    #El formato unicode de los emojis es \uXXXX
    # Patrón de expresión regular para encontrar emojis Unicode
    return emoji.is_emoji(caracter)


def q2_time(file_path: str) -> List[Tuple[str, int]]:
    #Inicializamos spark
    spark = create_spark_session()
    #Usamos solo el esquema solo con los campos necesarios para que el dataframe solo infiera esos campos y el tiempo de ejecución disminuya
    schema = StructType([
            StructField("content", StringType(), True)
        ])
    #Lectura del json
    df_universo = read_json(spark, file_path)

    #Registramos la función como UDF en spark
    es_emoji_udf = udf(es_emoji, BooleanType())

    df_emojis = df_universo.select(explode(split(col('content'), '')).alias('caracteres')).filter(es_emoji_udf(col('caracteres')))

    df_emojis_3 = df_emojis.groupby('caracteres').agg(count(lit(1)).alias("ctd_emoji")).orderBy("ctd_emoji", ascending=False)

    #Almacenamos el resultado en el formato pedido
    result = write_to_tuples(df_emojis_3, "caracteres", "ctd_emoji")
    #print(result)

    return result
    
q2_time("dbfs:/FileStore/shared_uploads/taboada.jose@pucp.pe/farmers_protest_tweets_2021_2_4.json")
