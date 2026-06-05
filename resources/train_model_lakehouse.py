#!/usr/bin/env python3
import argparse
import mlflow
import mlflow.spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, current_timestamp
from pyspark.ml.feature import Bucketizer, StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def build_spark():
    return (
        SparkSession.builder
        .appName("train_model_lakehouse")
        # Iceberg catalog against MinIO
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
        # S3A for direct model save
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-base", default="s3a://warehouse/artifacts/models")
    args = parser.parse_args()

    spark = build_spark()

    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.endpoint", "http://minio:9000")
    hconf.set("fs.s3a.access.key", "admin")
    hconf.set("fs.s3a.secret.key", "password")
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")

    mlflow.set_experiment("/flight_prediction/train_model_lakehouse")

    with mlflow.start_run():
        spark.sql("USE lakehouse.training_data")
        features = spark.table("lakehouse.training_data.flight_delay_features")
        features = features.select(
            "ArrDelay", "CRSArrTime", "CRSDepTime", "Carrier",
            "DayOfMonth", "DayOfWeek", "DayOfYear", "DepDelay",
            "Dest", "Distance", "FlightDate", "FlightNum", "Origin"
        )

        features = features.withColumn("Route", concat(features.Origin, lit("-"), features.Dest))

        arrival_bucketizer = Bucketizer(
            splits=[float("-inf"), -15.0, 0.0, 30.0, float("inf")],
            inputCol="ArrDelay",
            outputCol="ArrDelayBucket"
        )
        ml_bucketized_features = arrival_bucketizer.transform(features)

        index_columns = []
        for column in ["Carrier", "Origin", "Dest", "Route"]:
            idx = column + "_index"
            indexer = StringIndexer(inputCol=column, outputCol=idx, handleInvalid="keep")
            model_idx = indexer.fit(ml_bucketized_features)
            ml_bucketized_features = model_idx.transform(ml_bucketized_features).drop(column)
            index_columns.append(idx)

        numeric_columns = ["DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear"]
        vector_assembler = VectorAssembler(
            inputCols=numeric_columns + index_columns,
            outputCol="Features_vec"
        )
        final_df = vector_assembler.transform(ml_bucketized_features)
        for c in index_columns:
            final_df = final_df.drop(c)

        rfc = RandomForestClassifier(
            featuresCol="Features_vec",
            labelCol="ArrDelayBucket",
            predictionCol="Prediction",
            maxBins=4657,
            maxMemoryInMB=1024
        )
        model = rfc.fit(final_df)

        predictions = model.transform(final_df)
        evaluator = MulticlassClassificationEvaluator(
            predictionCol="Prediction",
            labelCol="ArrDelayBucket",
            metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)

        mlflow.log_param("maxBins", 4657)
        mlflow.log_param("maxMemoryInMB", 1024)
        mlflow.log_param("feature_count", len(numeric_columns) + len(index_columns))
        mlflow.log_metric("accuracy", accuracy)
        mlflow.spark.log_model(model, artifact_path="spark-model")

        out_path = f"{args.model_base}/rfc_flight_delays"
        model.write().overwrite().save(out_path)

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.model_registry")
        meta = spark.createDataFrame(
            [(out_path, float(accuracy))],
            ["model_path", "accuracy"]
        ).withColumn("_saved_at", current_timestamp())

        table_name = "lakehouse.model_registry.trained_models"
        if spark.catalog.tableExists(table_name):
            spark.sql(f"DROP TABLE {table_name}")

        meta.writeTo(table_name).using("iceberg").create()

    spark.stop()

if __name__ == "__main__":
    main()