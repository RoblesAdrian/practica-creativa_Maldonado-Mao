error id: file://<WORKSPACE>/flight_prediction/src/main/scala/es/upm/dit/ging/predictor/MakePrediction.scala:awaitAnyTermination.
file://<WORKSPACE>/flight_prediction/src/main/scala/es/upm/dit/ging/predictor/MakePrediction.scala
empty definition using pc, found symbol in pc: awaitAnyTermination.
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/functions/spark/streams/awaitAnyTermination.
	 -org/apache/spark/sql/functions/spark/streams/awaitAnyTermination#
	 -org/apache/spark/sql/functions/spark/streams/awaitAnyTermination().
	 -org/apache/spark/sql/types/spark/streams/awaitAnyTermination.
	 -org/apache/spark/sql/types/spark/streams/awaitAnyTermination#
	 -org/apache/spark/sql/types/spark/streams/awaitAnyTermination().
	 -com/datastax/spark/connector/spark/streams/awaitAnyTermination.
	 -com/datastax/spark/connector/spark/streams/awaitAnyTermination#
	 -com/datastax/spark/connector/spark/streams/awaitAnyTermination().
	 -spark/implicits/spark/streams/awaitAnyTermination.
	 -spark/implicits/spark/streams/awaitAnyTermination#
	 -spark/implicits/spark/streams/awaitAnyTermination().
	 -spark/streams/awaitAnyTermination.
	 -spark/streams/awaitAnyTermination#
	 -spark/streams/awaitAnyTermination().
	 -scala/Predef.spark.streams.awaitAnyTermination.
	 -scala/Predef.spark.streams.awaitAnyTermination#
	 -scala/Predef.spark.streams.awaitAnyTermination().
offset: 8122
uri: file://<WORKSPACE>/flight_prediction/src/main/scala/es/upm/dit/ging/predictor/MakePrediction.scala
text:
```scala
package es.upm.dit.ging.predictor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.streaming.Trigger
import com.datastax.spark.connector._

object MakePrediction {

  def buildSpark(): SparkSession = {
    SparkSession.builder()
      .appName("MakePredictionLakehouseKafka")
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
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .config("spark.hadoop.fs.s3a.access.key", "admin")
      .config("spark.hadoop.fs.s3a.secret.key", "password")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.cassandra.connection.host", sys.env.getOrElse("CASSANDRA_HOST", "cassandra"))
      .config("spark.cassandra.connection.port", sys.env.getOrElse("CASSANDRA_PORT", "9042"))
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    println("Flight predictor starting...")

    val spark = buildSpark()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    val requestTopic = sys.env.getOrElse("REQUEST_TOPIC", "flight-delay-ml-request")
    val responseTopic = sys.env.getOrElse("RESPONSE_TOPIC", "flight-delay-ml-response")
    val modelPath = sys.env.getOrElse("MODEL_PATH", "s3a://warehouse/artifacts/models/rfc_flight_delays")

    val hconf = spark.sparkContext.hadoopConfiguration
    hconf.set("fs.s3a.endpoint", "http://minio:9000")
    hconf.set("fs.s3a.access.key", "admin")
    hconf.set("fs.s3a.secret.key", "password")
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")

    val trainingDf = spark.table("lakehouse.training_data.flight_delay_features")
      .select(
        "Carrier", "Origin", "Dest", "ArrDelay", "DepDelay",
        "DayOfMonth", "DayOfWeek", "DayOfYear", "Distance", "FlightDate", "FlightNum"
      )
      .withColumn("Route", concat(col("Origin"), lit("-"), col("Dest")))

    val carrierIndexer = new StringIndexer()
      .setInputCol("Carrier")
      .setOutputCol("Carrier_index")
      .setHandleInvalid("keep")
      .fit(trainingDf)

    val originIndexer = new StringIndexer()
      .setInputCol("Origin")
      .setOutputCol("Origin_index")
      .setHandleInvalid("keep")
      .fit(trainingDf)

    val destIndexer = new StringIndexer()
      .setInputCol("Dest")
      .setOutputCol("Dest_index")
      .setHandleInvalid("keep")
      .fit(trainingDf)

    val routeIndexer = new StringIndexer()
      .setInputCol("Route")
      .setOutputCol("Route_index")
      .setHandleInvalid("keep")
      .fit(trainingDf)

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array(
        "DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear",
        "Carrier_index", "Origin_index", "Dest_index", "Route_index"
      ))
      .setOutputCol("Features_vec")
      .setHandleInvalid("keep")

    val rfc = RandomForestClassificationModel.load(modelPath)

    val requestSchema = new StructType()
      .add("Origin", StringType)
      .add("FlightNum", StringType)
      .add("DayOfWeek", IntegerType)
      .add("DayOfYear", IntegerType)
      .add("DayOfMonth", IntegerType)
      .add("Dest", StringType)
      .add("DepDelay", DoubleType)
      .add("Timestamp", TimestampType)
      .add("FlightDate", DateType)
      .add("Carrier", StringType)
      .add("UUID", StringType)
      .add("Distance", DoubleType)

    val requests = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", requestTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    val parsed = requests
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), requestSchema).as("flight"))
      .select("flight.*")
      .withColumn("Route", concat(col("Origin"), lit("-"), col("Dest")))
      .withColumn("FlightDate", to_date(col("FlightDate")))
      .withColumn("DayOfMonth", coalesce(col("DayOfMonth"), dayofmonth(col("FlightDate"))))
      .withColumn("DayOfYear", coalesce(col("DayOfYear"), dayofyear(col("FlightDate"))))
      .withColumn("DayOfWeek", coalesce(col("DayOfWeek"), dayofweek(col("FlightDate"))))
      .withColumn("Timestamp", coalesce(col("Timestamp"), current_timestamp()))

    val indexedCarrier = carrierIndexer.transform(parsed)
    val indexedOrigin = originIndexer.transform(indexedCarrier)
    val indexedDest = destIndexer.transform(indexedOrigin)
    val indexedRoute = routeIndexer.transform(indexedDest)

    val features = vectorAssembler.transform(indexedRoute)

    val scored = rfc.transform(features)

    val responses = scored.select(
      col("UUID"),
      col("Origin"),
      col("Dest"),
      col("Carrier"),
      col("FlightNum"),
      col("DepDelay"),
      col("Distance"),
      col("DayOfWeek"),
      col("DayOfMonth"),
      col("DayOfYear"),
      col("Timestamp"),
      col("prediction").cast("int").as("Prediction")
    ).withColumn(
      "value",
      to_json(struct(
        col("UUID"),
        col("Origin"),
        col("Dest"),
        col("Carrier"),
        col("FlightNum"),
        col("DepDelay"),
        col("Distance"),
        col("DayOfWeek"),
        col("DayOfMonth"),
        col("DayOfYear"),
        col("Timestamp"),
        col("Prediction")
      ))
    ).selectExpr("CAST(NULL AS STRING) AS key", "CAST(value AS STRING) AS value")

    val responseDf = scored.select(
      col("UUID").as("uuid"),
      col("Origin").as("origin"),
      col("Dest").as("dest"),
      col("Carrier").as("carrier"),
      col("FlightNum").as("flightnum"),
      col("DepDelay").as("depdelay"),
      col("Distance").as("distance"),
      col("DayOfWeek").as("dayofweek"),
      col("DayOfMonth").as("dayofmonth"),
      col("DayOfYear").as("dayofyear"),
      col("Timestamp").as("timestamp"),
      col("prediction").cast("int").as("prediction")
    )

    val kafkaQuery = responses.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", responseTopic)
      .option("checkpointLocation", "/tmp/checkpoints/make_prediction_kafka")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    val cassandraQuery = responseDf.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF
          .write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", "agile_data_science")
          .option("table", "flight_delay_ml_results")
          .mode("append")
          .save()
      }
      .outputMode("append")
      .option("checkpointLocation", "/tmp/checkpoints/make_prediction_cassandra")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()
    
    spark.streams.awaitAnyTermin@@ation()
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: awaitAnyTermination.