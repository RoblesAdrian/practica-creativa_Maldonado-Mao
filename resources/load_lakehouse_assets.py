#!/usr/bin/env python3
import argparse
import bz2
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


def build_spark():
    return (
        SparkSession.builder
        .appName("load_lakehouse_assets")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.client.region", "eu-south-2")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", "admin")
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", "password")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .getOrCreate()
    )


def load_bz2_jsonl(path):
    rows = []
    with bz2.open(path, "rt") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", default="/workspace/data/simple_flight_delay_features.jsonl.bz2")
    args = parser.parse_args()

    features_path = Path(args.features)
    if not features_path.exists():
        raise FileNotFoundError(f"Missing input file: {features_path}")

    spark = build_spark()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.training_data")

    feature_rows = load_bz2_jsonl(features_path)
    features_df = spark.createDataFrame(feature_rows)
    features_df = features_df.withColumn("_source_file", lit(str(features_path)))
    features_df = features_df.withColumn("_ingested_at", current_timestamp())

    features_df.writeTo("lakehouse.training_data.flight_delay_features").using("iceberg").createOrReplace()

    spark.stop()


if __name__ == "__main__":
    main()