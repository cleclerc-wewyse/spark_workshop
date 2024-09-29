from pyspark.sql import SparkSession
import pyspark.sql.functions as F

"""
Exercice sur l'interet de cacher ses dataframes, et au bon moment

=> On joint les données des evenements et des trajets. 
On veut ensuite écrire séparement les données pour le quartier
de Manhattan et celui du bronx. 


"""


if __name__ == "__main__":

    spark = SparkSession.builder.appName("exercice 5")\
                        .getOrCreate()
    

    # desactive l'AQE pour montrer sans optimisation de spark
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    # trajet de taxi dans new york
    df_taxi = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_taxi_trips")\

    # table reference pour identifier les quartiers des trajets de taxi
    df_zone = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/taxi_zone")

    # les evenements qui ont eu lieu dans new york
    df_events = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_events")\
                    .withColumn("start_date", F.to_date('start_datetime'))

    df_weather = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_weather")

    # jointure pour avoir le nom des quartiers des depart & arrives des trajets de taxi
    df_taxi_borough = df_taxi.join(df_zone, df_taxi["PULocationID"] == df_zone["LocationID"], "left")\
                        .withColumn('pickup_date', F.to_date('tpep_pickup_datetime'))\
                        .withColumnRenamed("borough", "borough_taxi")\
                        .select("pickup_date", "borough_taxi", "payment_type", "fare_amount", "passenger_count",
                               "trip_distance")
    
    
    df_events_trips = df_events.join(df_taxi_borough, (df_events["start_date"] == df_taxi_borough["pickup_date"])
                                 & (df_events["event_borough"] == df_taxi_borough["borough_taxi"]), "left")\
                                 .drop("borough_taxi")
    
    # df_events_weather = df_events_trips.join(df_weather, (df_events_trips["start_date"] == df_weather["DATE"])
    #                              & (df_events_trips["event_borough"] == df_weather["borough"]), "left")\
    #                              .drop("borough")
    

    df_manhattan = df_events_trips.filter("event_borough = 'Manhattan'")

    df_manhattan.write.mode('overwrite').format("parquet").save("/Users/cleclerc/Documents/spark_workshop/data/ex5_manhattan")


    df_bronx = df_events_trips.filter("event_borough = 'Bronx'")

    df_bronx.write.mode('overwrite').format("parquet").save("/Users/cleclerc/Documents/spark_workshop/data/ex5_bronx")

    

    