from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("IPL Bronze Layer Job") \
        .getOrCreate()

    df_raw = spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load("/opt/spark-app/data/raw/ipl.csv")

    if "_c0" in df_raw.columns:
        df_raw = df_raw.drop("_c0")

    df_raw.write.mode("overwrite") \
        .parquet("/opt/spark-app/data/bronze/ipl")

    spark.stop()

if __name__ == "__main__":
    main()