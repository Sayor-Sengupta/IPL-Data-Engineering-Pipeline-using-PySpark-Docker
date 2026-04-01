from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def main():
    spark = SparkSession.builder \
        .appName("IPL Silver Layer") \
        .getOrCreate()

    df = spark.read.parquet('/opt/spark-app/data/bronze/ipl')

    df = df.withColumn("match_type", col("match_type").cast("string"))

    df = df.withColumn("is_boundary", when(col("runs_batter") > 4, 1).otherwise(0))

    df = df.withColumn("is_wicket", when(col("wicket_kind").isNotNull(), 1).otherwise(0))

    df = df.filter(col("valid_ball") == 1)

    df_silver = df.select(
        "match_id", "batting_team", "bowling_team",
        "batter", "bowler", "over", "ball",
        "runs_batter", "runs_total",
        "is_boundary", "is_wicket",
        "venue", "year"
    )

    df_silver.write.mode("overwrite") \
        .parquet("/opt/spark-app/data/silver/ipl_cleaned")

    spark.stop()

if __name__ == "__main__":
    main()