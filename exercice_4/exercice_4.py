from pyspark.sql import SparkSession
import pyspark.sql.functions as F

"""
Exercice sur le data skewing : on a une mauvaise répartition des données sur la clé
de jointure. comment on fix ça ? 

"""


if __name__ == "__main__":

    spark = SparkSession.builder.appName("rewrite_nyc_events")\
                        .getOrCreate()
    
# trajet de taxi dans new york
df_taxi = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_taxi_trips")

# table reference pour identifier les quartiers des trajets de taxi
df_zone = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/taxi_zone")

df_events = spark.read.parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_events")\
                .withColumn('start_date', F.to_date("start_datetime"))

# jointure pour avoir les quartiers pour les trajets
df_taxi_borough = df_taxi.join(df_zone, df_taxi["PULocationID"] == df_zone["LocationID"], "left").withColumn('pickup_date', F.to_date('tpep_pickup_datetime'))

# jointure entre les trajets de taxi et les evenements dans new york
df_joined = df_taxi_borough.join(df_events, (df_events["start_date"] == df_taxi_borough["pickup_date"]) & (df_events["event_borough"] == df_taxi_borough["Borough"]), "left")

# rajout d'une colonne pour savoir s'il y avait un evenement dans cette ville ou non 
df_joined_col = df_joined.withColumn('is_event', F.when(F.col('event_name').isNotNull(), "yes").otherwise("no"))
df_joined_col.groupBy("Borough", "is_event").agg(F.count("*")).explain()