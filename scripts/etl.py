import os
from pyspark.sql import SparkSession


def get_spark_session(app_name="HomeCreditETL"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()


def convert_csvs_to_parquet(raw_dir, processed_dir):
    spark = get_spark_session()

    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)

    for file in os.listdir(raw_dir):
        if file.endswith(".csv"):
            file_path = os.path.join(raw_dir, file)
            df_name = file.replace(".csv", "")
            print(f"🔄 Converting: {file}")

            try:
                df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
                df.write.mode("overwrite").parquet(os.path.join(processed_dir, f"{df_name}.parquet"))
                print(f"Saved: {df_name}.parquet")
            except Exception as e:
                print(f"Failed to process {file}: {e}")

    print("All CSV files converted to Parquet successfully.")

