# Databricks notebook source
from typing import List, Tuple, TypeVar
from pyspark.sql import SparkSession, DataFrame

# Definimos un tipo parametrizado para los tipos de datos de los campos
T = TypeVar('T')

def create_spark_session(app_name: str= 'LATAM CHALLENGUE') -> SparkSession:
    """
    Crea y devuelve una sesión de Spark.
    Se le pasa el app_name para que cree su sesión con el nombre deseado
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def read_json(spark: SparkSession, file_path: str, schema= None) -> DataFrame:
    """
    Lee un archivo JSON y devuelve un DataFrame de Spark.
    """
    if schema == None:
        df = spark.read.json(file_path)
    else:
        df=spark.read.schema(schema).json(file_path)
    return df

def write_to_tuples(df: DataFrame, field1: str, field2: str, top_n: int= 10) -> List[Tuple[T, T]]:
    """
    Convierte un DataFrame de Spark en una lista de tuplas, seleccionando los campos especificados.
	Se pon por defecto top_n = 10, ya que los 3 ejercicios nos piden el top 10 de X requerimiento
    """
    # Utilizamos el método take para obtener los primeros top_n elementos como una lista
    tuples_list = df.select(field1, field2).take(top_n)
    
    # Convertimos la lista de Row en una lista de tuplas
    tuples_list = [(row[field1], row[field2]) for row in tuples_list]
    
    return tuples_list
