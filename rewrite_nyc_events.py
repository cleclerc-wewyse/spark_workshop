from pyspark.sql import SparkSession
import pyspark.sql.functions as F


if __name__ == "__main__":

    spark = SparkSession.builder.appName("rewrite_nyc_events")\
                        .getOrCreate()
    
    df_nyc_events = spark.read.csv("/Users/cleclerc/Downloads/NYC_Permitted_Event_Information_-_Historical_20240910.csv",
                                sep=",", header=True)
    
    df_format_dates = df_nyc_events.withColumn('start_datetime', 
                                            F.unix_timestamp(F.col('Start Date/Time'), "MM/dd/yyyy hh:mm:ss a")\
                                            .cast('timestamp'))\
                                    .withColumn('end_datetime', 
                                            F.unix_timestamp(F.col('End Date/Time'), "MM/dd/yyyy hh:mm:ss a")\
                                            .cast('timestamp'))\
                                    .drop("Start Date/Time", "End Date/Time")
    
    df_rename_cols = df_format_dates.withColumn('event_id', F.col('Event ID').cast('integer'))\
                                    .withColumn('event_name', F.col('Event Name'))\
                                    .withColumn('event_agency', F.col('Event Agency'))\
                                    .withColumn('event_type', F.col('Event Type'))\
                                    .withColumn('event_borough', F.col('Event Borough'))\
                                    .withColumn('event_location', F.col("Event Location"))\
                                    .withColumn('street_closure_type', F.col('Street Closure Type'))\
                                    .drop('Event ID', 'Event Name', 'Event Agency', 'Event Type', 'Event Borough',
                                          'Start Date/Time', 'End Date/Time', 'Event Location', 'Event Street Side',
                                          'Street Closure Type', 'Community Board', 'Police Precinct')
    
    df_processed = df_rename_cols.select('event_id', 'event_name', 'start_datetime', 'end_datetime',
                                        'event_agency', 'event_type', 'event_borough', 'event_location',
                                        'street_closure_type')
    
    df_processed.orderBy(F.col('start_datetime').desc()).show()

    df_processed.printSchema()

    df_processed.write.mode('overwrite').parquet('/Users/cleclerc/Documents/spark_workshop/data/nyc_events')