package com

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, json_tuple, regexp_extract, timestamp_seconds, to_date}
import org.apache.spark.sql.types._

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

object VTdataLoad6 extends App{

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

  val kafkaTopic = "vt"

  //  // Sample data schema (replace with your actual data)
  case class DataRecord(video_id: String, views: String, likes: String, dislikes: String, load_date: String)
  //
  val gcslandingPath = "gs://yvideos_gcp_poc/landing/USvideos.csv"
  val gcsPath = "gs://yvideos_gcp_poc/source/"


  // Your DataFrame (assuming df is already created)
  val date = "2017-11-14"
  val df = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")
    .option("escape", "\\")
    .option("multiline", "true")
    .csv(gcslandingPath)
    .withColumn("load_date", to_date(col("trending_date"), "yy.dd.MM"))
    .select("video_id", "title", "load_date")
    .filter(col("load_date")=== date)

  val filteredDFJson = df.selectExpr("CAST(video_id AS STRING) as key", "to_json(struct(*)) AS value")

  // Write the filtered data to a Kafka topic
  val chunkSize = 10000
  val totalRecords = df.count()
  println("Total records to be sent to topic", totalRecords)

  filteredDFJson
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
    .option("topic", kafkaTopic)
    .save()
  spark.stop()





}
