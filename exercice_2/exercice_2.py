from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

"""
Exercice sur l'opti broadcast join au lieu d'un sortmergejoin
On a à nouveau le dataframe résultat de la jointure entre
les trajets de taxi et la table de référence des quartiers
On a deja un broadcastHashJoin la, l'idée est de montrer un cas 
de figure ou on a un sortMergeJoin sans opti puis un broadcastHashJoin
apres opti

Ici, on veut afficher le nombre de trajets de taxi qui sont allé vers un évements
qui a eu lieu dans le quartier de Manhattan en mars 2022
"""


if __name__ == "__main__":

    spark = SparkSession.builder.appName("exercice_2")\
                        .getOrCreate()
    
    # desactive l'AQE pour montrer sans optimisation de spark
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    # trajet de taxi dans new york
    df_taxi = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_taxi_trips")

    # table reference pour identifier les quartiers des trajets de taxi
    df_zone = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/taxi_zone")

    # les evenements qui ont eu lieu dans new york
    df_events = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_events")

    # jointure pour avoir le nom des quartiers des depart & arrives des trajets de taxi
    df_taxi_borough = df_taxi.join(df_zone, df_taxi["PULocationID"] == df_zone["LocationID"], "left")\
                        .withColumn('pickup_date', F.to_date('tpep_pickup_datetime'))


    df_events_filtered = df_events.filter("event_borough = 'Manhattan' and DATE(start_datetime) >= '2020-04-01' and DATE(end_datetime) < '2022-05-01'").withColumn('start_date', F.to_date("start_datetime")).withColumn('end_date', F.to_date("end_datetime"))

    df_final = df_events_filtered.join(df_taxi_borough, (df_events_filtered["start_date"] == df_taxi_borough["pickup_date"]) & (df_events_filtered["event_borough"] == df_taxi_borough["Borough"]), "left")

    # df_final.explain()

    df_final.show()