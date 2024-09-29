from pyspark.sql import SparkSession
import pyspark.sql.functions as f




if __name__ == "__main__":

    spark = SparkSession.builder.appName("weather stations nyc").getOrCreate()

    df_stations = 