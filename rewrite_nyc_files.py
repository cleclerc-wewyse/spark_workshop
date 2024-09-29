from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import LongType, DoubleType, StringType, TimestampType

import argparse

def get_args():
    # Initialisation de l'analyseur d'arguments
    parser = argparse.ArgumentParser(description='Process NYC Taxi data for a specific year and month.')
    parser.add_argument('--annee', type=str, required=True, help="L'année pour le filtrage des données")
    parser.add_argument('--mois', type=str, required=True, help="Le mois pour le filtrage des données")

    # Parse les arguments
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    # loop over annee/mois to fix column type issues and have consistent schema. 
    # also write data in parquet format and partition by annee and mois

    args = get_args()

    spark = SparkSession.builder.appName("rewrite_nyc").getOrCreate()

    print(f"/Users/cleclerc/Downloads/nyc_trips/yellow_tripdata_{args.annee}-{args.mois}*")

    # Lire les données Parquet sans appliquer de schéma pour commencer
    df_nyc_trips = spark.read.parquet(f"/Users/cleclerc/Downloads/nyc_trips/yellow_tripdata_{args.annee}-{args.mois}*")

    # Appliquer un cast sur chaque colonne pour le bon type
    df_nyc_trips_casted = (df_nyc_trips
        .withColumn("VendorID", df_nyc_trips["VendorID"].cast(LongType()))
        .withColumn("tpep_pickup_datetime", df_nyc_trips["tpep_pickup_datetime"].cast(TimestampType()))
        .withColumn("tpep_dropoff_datetime", df_nyc_trips["tpep_dropoff_datetime"].cast(TimestampType()))
        .withColumn("passenger_count", df_nyc_trips["passenger_count"].cast(DoubleType()))
        .withColumn("trip_distance", df_nyc_trips["trip_distance"].cast(DoubleType()))
        .withColumn("RatecodeID", df_nyc_trips["RatecodeID"].cast(DoubleType()))
        .withColumn("store_and_fwd_flag", df_nyc_trips["store_and_fwd_flag"].cast(StringType()))
        .withColumn("PULocationID", df_nyc_trips["PULocationID"].cast(LongType()))
        .withColumn("DOLocationID", df_nyc_trips["DOLocationID"].cast(LongType()))
        .withColumn("payment_type", df_nyc_trips["payment_type"].cast(LongType()))
        .withColumn("fare_amount", df_nyc_trips["fare_amount"].cast(DoubleType()))
        .withColumn("extra", df_nyc_trips["extra"].cast(DoubleType()))
        .withColumn("mta_tax", df_nyc_trips["mta_tax"].cast(DoubleType()))
        .withColumn("tip_amount", df_nyc_trips["tip_amount"].cast(DoubleType()))
        .withColumn("tolls_amount", df_nyc_trips["tolls_amount"].cast(DoubleType()))
        .withColumn("improvement_surcharge", df_nyc_trips["improvement_surcharge"].cast(DoubleType()))
        .withColumn("total_amount", df_nyc_trips["total_amount"].cast(DoubleType()))
        .withColumn("congestion_surcharge", df_nyc_trips["congestion_surcharge"].cast(DoubleType()))
        .withColumn("airport_fee", df_nyc_trips["airport_fee"].cast(DoubleType()))
    )

    # Créer les colonnes "annee" et "mois" à partir de "tpep_pickup_datetime"
    df_nyc_trips_casted = df_nyc_trips_casted.withColumn("annee", F.year(df_nyc_trips_casted["tpep_pickup_datetime"]))
    df_nyc_trips_casted = df_nyc_trips_casted.withColumn("mois", F.month(df_nyc_trips_casted["tpep_pickup_datetime"]))

    # Écrire les données au format Parquet, partitionnées par "annee" et "mois", en mode overwrite
    df_nyc_trips_casted.write.mode("append").partitionBy("annee", "mois")\
                        .parquet("/Users/cleclerc/Documents/spark_workshop/data/nyc_taxi_trips")