from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import LongType, DoubleType, StringType, TimestampType



# df_events.withColumn('bla', F.concat(F.lit('test1'), F.col('End Date/Time'), F.lit('test2')))\
#         .withColumn('pouet', F.lit('11/22/2017 08:00:00 PM'))\
#         .withColumn('to_tmp',F.unix_timestamp(F.col('pouet'), "MM/dd/yyyy HH:mm:ss a").cast('timestamp'))\
#         .select('End Date/Time', 'bla', 'to_tmp', 'pouet').show(truncate=False)


if __name__ == '__main__':

    #instantiate sparkSession
    spark = SparkSession.builder.appName("spark_workshop_E1").getOrCreate()

    df_nyc_trips = spark.read.parquet('/Users/cleclerc/Downloads/nyc_trips')

    print(df_nyc_trips.count())

    df_nyc_trips.show()

df_nyc_trips = spark.read.parquet('/Users/cleclerc/Downloads/nyc_trips/yellow_tripdata_2020*')


 df_annee = df_nyc_trips_casted.withColumn('annee', F.year('tpep_dropoff_datetime'))

df_annee_mois = df_annee.withColumn('mois', F.month('tpep_dropoff_datetime'))

df_annee_mois.write.mode('overwrite').format('parquet').partitionBy('annee', 'mois').save('data/nyc_taxi_trips')