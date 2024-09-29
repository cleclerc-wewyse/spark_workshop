from pyspark.sql import SparkSession
import pyspark.sql.functions as F

"""
Exercice sur l'optimisation de la lecture des données, partitionfilter, pushdwonfilter : 
Le partitionFilter : ici on va filtrer sur l'année (2023) qui est une clé de partition
Le pushdown filter : ici on va sélectionner le nombre de passager supérieur à 1 
"""

"""
Correction : 
Deja le filtre peut etre fait avant la jointure, meme si ici il s'agit d'un broadcastjoin, 
sur de faible volume donc peu d'impact.

Ensuite, le filtre sur l'année doit etre fait sur la colonne de partitionnement, qui est "annee",
ce qui permet d'avoir un partitionfilter en n'ayant que les données de cette année de lu.

Enfin, le filtre sur le "passenger_count" ne permet pas de faire un pushdownfilter, à cause 
du cast("int") qui modifie la colonne est donc besoin de la lire completement.
En le supprimant le pushdownfilter est présent. 
"""


if __name__ == "__main__":

    spark = SparkSession.builder.appName("correction_1")\
                        .getOrCreate()
    

    # trajet de des taxi de new york 
    df_taxi = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_taxi_trips")

    # table reference des zones (quartiers) de new york
    df_zone = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/taxi_zone")

    df_taxi_filtered = df_taxi.filter((F.col("annee") == '2023') &
                                (F.col("passenger_count") > F.lit("1.0")))

    # jointure entre les trajets et les zone, sur leur l'identifiant 
    # de la zone de récupération du client
    df_joined = df_taxi_filtered.join(df_zone, df_taxi_filtered["PULocationID"] == df_zone["LocationID"], "left")

    df_joined.explain()
