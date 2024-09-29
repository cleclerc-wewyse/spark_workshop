from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

"""
Exercice sur l'optimisation de la lecture des données : 
Le partitionFilter : ici on va filtrer sur l'année (2023) qui est une clé de partition
Le pushdown filter : ici on va sélectionner le nombre de passager supérieur à 1 
"""


if __name__ == "__main__":

    spark = SparkSession.builder.appName("exercice_1")\
                        .getOrCreate()

    # trajet de des taxi de new york 
    df_taxi = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_taxi_trips")

    # table reference des zones (quartiers) de new york
    df_zone = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/taxi_zone")

    # jointure entre les trajets et les zone, sur leur l'identifiant 
    # de la zone de récupération du client
    df_joined = df_taxi.join(df_zone, df_taxi["PULocationID"] == df_zone["LocationID"], "left")

    # filtre des données jointures sur l'année du trajet ainsi que le nombre de participants
    df_final = df_joined.filter((F.year("tpep_pickup_datetime") == '2023') & (F.col("passenger_count").cast("int") > F.lit("1")))

    df_final.explain()

    