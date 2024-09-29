from pyspark.sql import SparkSession
import pyspark.sql.functions as F

"""
Exercice sur l'optimisation des fichiers à écrire
Optimisation sur le partitionnement des fichiers en fonction
de l'utilisation des données par les utilisateurs
et optimiser petits fichiers

"""


if __name__ == "__main__":

    spark = SparkSession.builder.appName("rewrite_nyc_events")\
                        .getOrCreate()
    
    df_events = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_events")

    df_events = df_events.repartition("event_borough")

    df_events.write.partitionBy("event_borough").mode("overwrite").format("parquet")\
            .save("/Users/cleclerc/Documents/spark_workshop/data/ex6_events")