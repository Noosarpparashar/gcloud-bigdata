package com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, json_tuple, regexp_extract, timestamp_seconds}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

object VvldCosnume8 extends App{

  val conf = new SparkConf()
    .setAppName("ReadParquet")
    .set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    .set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    .set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "src/main/resources/gcloud-creds.json")
    .set("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.my_catalog.type", "hive")


  val spark = SparkSession.builder
    .master("local[*]").config(conf).getOrCreate()

  import spark.implicits._

  //val kafkaBootstrapServers = "http://34.125.26.43:9092,http://34.125.26.43:9093,http://34.125.26.43:9094"
  val kafkaBootstrapServers = "http://localhost:9092,http://localhost:9093,http://localhost:9094"

  val topic = "vvld"
  val startingOffsets = "latest"
  val gcsCheckPointPath = "gs://yvideos_gcp_poc/source/vvld/checkpoints"
  val gcsLoadPath = "gs://yvideos_gcp_poc/source/vvld/delta"

  // Assuming your JSON has the structure you provided
  val schema = StructType(Seq(
    StructField("video_id", StringType),
    StructField("views", IntegerType),
    StructField("likes", IntegerType),
    StructField("dislikes", IntegerType),
    StructField("load_date", StringType)
  ))


  println("1111")
  val kafkaDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
  println("2222")
  val jsonParsedDF = kafkaDF
    .selectExpr("CAST(value as STRING) as jsonString") // Cast Kafka value to a string
    .withColumn("jsonString", regexp_extract(col("jsonString"), "\\{.*\\}", 0))
    .select(
      json_tuple(col("jsonString"), "video_id", "views", "likes","dislikes","load_date")
      //.as("video_id", "title", "publish_time")
    ).toDF("video_id", "views","likes","dislikes", "load_date")


  val icebergTable = "default.yvideos_vvld_delta" // Your target Iceberg table

  jsonParsedDF.writeStream
    .partitionBy("load_date") // Optional if you want to partition the Iceberg table by load_date
    .format("iceberg") // Use the Iceberg format
    .outputMode("append") // Append mode for streaming data
    .option("checkpointLocation", gcsCheckPointPath) // Checkpointing path
    .option("path", icebergTable) // Specify Iceberg table as the output path
    .trigger(Trigger.Once()) // Process the data once and stop
    .start()
    .awaitTermination()


}
