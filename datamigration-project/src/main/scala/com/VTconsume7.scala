package com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, json_tuple, regexp_extract, timestamp_seconds}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

object VTconsume7 extends App{

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


  val spark = SparkSession.builder
    .master("local[*]").config(conf).getOrCreate()

  import spark.implicits._

  //val kafkaBootstrapServers = "http://34.125.26.43:9092,http://34.125.26.43:9093,http://34.125.26.43:9094"
  val kafkaBootstrapServers = "http://localhost:9092,http://localhost:9093,http://localhost:9094"

  val topic = "vt"
  val startingOffsets = "latest"
  val gcsCheckPointPath = "gs://yvideos_gcp_poc/source/vt/checkpoints"
  val gcsLoadPath = "gs://yvideos_gcp_poc/source/vt/delta"

  // Assuming your JSON has the structure you provided
  val schema = StructType(Seq(
    StructField("video_id", StringType),
    StructField("title", StringType),
    StructField("load_date", StringType)
  ))

  val dateFormat = "yyyyMMdd_HHmmss"
  private val dateTimeFormatter = DateTimeFormatter.ofPattern(dateFormat)
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
      json_tuple(col("jsonString"), "video_id", "title", "load_date")
        //.as("video_id", "title", "publish_time")
    ).toDF("video_id", "title", "load_date")


  jsonParsedDF.writeStream
 //   .format("console")
//    .option("truncate", false)
//    .outputMode("append")
//    .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("30 seconds"))
//    .start()
//    .awaitTermination()
 .partitionBy("load_date")
    .format("parquet")
    .outputMode("append")
    .option("checkpointLocation", gcsCheckPointPath)
    .option("path", gcsLoadPath)
    .trigger(Trigger.Once())
    .start()
    .awaitTermination()


}
