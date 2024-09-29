from pyspark.sql import SparkSession
import pyspark.sql.functions as F

"""
Exercice sur l'ordre des requetes qui pourraient
etre optimiser (sans perdre/fausser les transformatiosn voulues)

"""


if __name__ == "__main__":

    spark = SparkSession.builder.appName("exercice_3")\
                        .getOrCreate()
    
    # desactive l'AQE pour montrer sans optimisation de spark
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    # trajet de taxi dans new york
    df_taxi = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_taxi_trips")

    # table reference pour identifier les quartiers des trajets de taxi
    df_zone = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/taxi_zone")

    # les evenements qui ont eu lieu dans new york
    df_events = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_events")

    # les données meteos par jour et quartier
    df_weather = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_weather")

    # join sur le pickup location id pour avoir les quartiers des depart des trajets
    # renomme la colonne borough pour indiquer que c'est le quartier des pickup
    df_pu_borough = df_taxi.join(df_zone, df_taxi["PULocationID"] == df_zone["LocationID"], "left")\
                            .withColumnRenamed("Borough", "Pick_up_borough")
