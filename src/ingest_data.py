from pyspark.sql import SparkSession

def main():
    print("Starting ingestion...")

    # Prefer env vars so the same code runs in dev/stage/prod
    input_path = os.environ.get(
        "INPUT_PATH",
        "abfss://raw-data@mystorage.dfs.core.windows.net/uploads/"
    )

    # Keep these separate
    schema_location = os.environ.get(
        "SCHEMA_LOCATION",
        "abfss://checkpoints@mystorage.dfs.core.windows.net/ingestion_job/schema/"
    )
    checkpoint_location = os.environ.get(
        "CHECKPOINT_LOCATION",
        "abfss://checkpoints@mystorage.dfs.core.windows.net/ingestion_job/checkpoint/"
    )

    target_table = os.environ.get(
        "TARGET_TABLE",
        "main.default.raw_ingested_data"
    )

    spark = SparkSession.builder.appName("IngestionJob").getOrCreate()

    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", schema_location)
            # good defaults for CSV
            .option("header", "true")
            .option("inferColumnTypes", "true")
            .load(input_path)
    )

    query = (
        df.writeStream
          .format("delta")
          .option("checkpointLocation", checkpoint_location)
          .trigger(availableNow=True)
          .toTable(target_table)
    )

    # Critical for jobs: wait for availableNow run to complete
    query.awaitTermination()

    print("Ingestion completed.")

if __name__ == "__main__":
    import os
    main()