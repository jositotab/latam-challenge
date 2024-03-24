# Databricks notebook source
# MAGIC %run /src/utils/functions

# COMMAND ----------

from typing import List, Tuple
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql.functions import col, to_date, lit, count, row_number, substring

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
        #Creamos sesión de spark:
        spark = create_spark_session()

        #Usamos solo el esquema solo con los campos necesarios para que el dataframe solo infiera esos campos y el tiempo de ejecución disminuya
        schema = StructType([
                StructField("date", StringType(), True),
                StructField("user", StructType([
                    StructField("username", StringType(), True)
                ]), True)
            ])

        #Leemos el .json
        df_json = read_json(spark, file_path, schema)

        #Realizamos las tranformaciones necesarias
        ##Paso1: Quedarnos solo con los campos necesarios (date, user.username) y darle el formato a la fecha
        df_universo = df_json.select(
                                to_date(substring(col("date"),1,10).cast("date"), 'yyyy-mm-dd').alias("date_dia"),
                                col("user.username").alias("username")
        )
        #df_universo.display()
        #print(df_universo.columns)

        df_universo.cache() #usamos .cache() en este dataframe ya que vamos a volver a utilizarlo más adelante y eso nos ayuda en el tiempo de ejecución

        ##Paso2: Con funciones de ventana obtenemos una lista con las 10 fechas más twitteadas y almacenamos en lista 
        window_spec = Window.partitionBy("ctd_x_fecha")
        window_specb = Window.partitionBy("ctd_x_fecha_username")

        df_ctd_fecha_01 = df_universo.groupBy("date_dia").agg(count(lit(1)).alias('ctd_x_fecha'))

        #print('ctd_x_fecha_max')
        #df_ctd_fecha_01.orderBy(col('ctd_x_fecha').desc()).show()

        df_ctd_fecha_02 = df_ctd_fecha_01.select( 
                        col("date_dia"),
                        col("ctd_x_fecha"),
                        row_number().over(window_spec.orderBy(col("ctd_x_fecha").desc())).alias("top_n"))
                        
        df_ctd_fecha_03 = df_ctd_fecha_02.where(col("top_n") <= 10)

        #df_ctd_fecha_03.show()

        top_10_fechas_list = [c.date_dia for c in df_ctd_fecha_03.collect()] #usamos collect porque solo son 10 registros, si fuere más optaríamos por otra forma de recolectar los datos como take
        #print(top_10_fechas_list)

        ##Paso3: Con funciones de ventana obtenemos de misma manera los usuarios más frecuentes en esas 10 fechas

        df_ctd_fecha_user_01 = df_universo.where(col("date_dia").isin(*[c for c in top_10_fechas_list])).\
                groupBy("date_dia", "username").agg(count(lit(1)).alias('ctd_x_fecha_username'))

        df_ctd_fecha_user_02 = df_ctd_fecha_user_01.select(
                        col("date_dia"),
                        col("username"),
                        col("ctd_x_fecha_username"),
                        row_number().over(window_specb.orderBy(col("ctd_x_fecha_username").desc())).alias("top_n"))
                        
        df_ctd_fecha_user_03 = df_ctd_fecha_user_02.where(col("top_n") == 1)

        ##Paso4: Almacenamos el resultado en el formato pedido
        result = write_to_tuples(df_ctd_fecha_user_03, "date_dia", "username")

        #print(result)

        return result

#q1_time("dbfs:/FileStore/shared_uploads/taboada.jose@pucp.pe/farmers_protest_tweets_2021_2_4.json")
