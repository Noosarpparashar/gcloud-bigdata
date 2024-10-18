package com

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object VideoIdDailyFileGen3 extends App {

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



  // Specify the GCS bucket path where the data should be written
  val gcslandingPath = "gs://yvideos_gcp_poc/landing/USvideos.csv"

  val videoUrlsDF = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")
    .option("escape", "\\")
    .option("multiline", "true")
    .csv(gcslandingPath)
    .select("video_id", "thumbnail_link")
    .withColumnRenamed("thumbnail_link", "url")

  // Select the column from which you want to pick random records
  val columnName = "video_id"  // Replace with your actual column name
  val columnDataDF = videoUrlsDF.select(columnName).distinct() // Get distinct column values if needed

  // Randomly sample 1000 records from the selected column
  val sampledDataDF = columnDataDF.orderBy(rand()).limit(1000)

  // Convert DataFrame to RDD of strings, then create a single string with space-separated values
  val sampledRDD = sampledDataDF.rdd.map(row => row.getString(0))
  val spaceSeparatedRDD = sampledRDD.mapPartitions(iter => Iterator(iter.mkString(" ")))

  // Define the format for the output file name (yyyyMMddHH)
  val currentDateTime = LocalDateTime.now()
  val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val formattedDate = currentDateTime.format(formatter)

  // Define the dynamic output path
  val outputPath = s"gs://yvideos_gcp_poc/source/dailyVideoIdFiles/$formattedDate"



  // Save the result as a single text file
  spaceSeparatedRDD.coalesce(1).saveAsTextFile(outputPath)

  // Stop Spark session
  spark.stop()
}

