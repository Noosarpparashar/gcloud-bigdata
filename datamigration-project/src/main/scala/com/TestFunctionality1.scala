package com
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}
object TestFunctionality1 extends App{

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

  val bigDataOSSVersion = getClass.getPackage.getImplementationVersion
  println("com.google.cloud.bigdataoss version: " + bigDataOSSVersion)


  // Specify the GCS bucket path where the data should be written
  val gcslandingPath = "gs://yvideos_gcp_poc/landing/USvideos.csv"
  val gcsPath = "gs://yvideos_gcp_poc/source/temp"


  // Your DataFrame (assuming df is already created)
  val df = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")
    .option("escape", "\\")
    .option("multiline", "true")
    .csv(gcslandingPath)
    .filter(col("category_id")===1)
  println("iamonline1")


  val date = "2017-11-15"
  println("iamonline2")
  df.show()

  // Write the DataFrame to the specified GCS path in Parquet format, partitioned by category_id
  df
    .withColumn("load_date", to_date(col("trending_date"), "yy.dd.MM"))
    .select("video_id", "views","likes","dislikes", "load_date")
    .filter(col("load_date")=== date)
    .write
    .partitionBy("load_date")  // Match the partitioning in Hive
    .mode("append") // or "append" depending on your use case
    .parquet(gcsPath)
  println("iamonline3")

  // Stop the SparkSession
  spark.stop()


}





