import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, to_date}

object TestFunctionality extends App {

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

    // Iceberg configurations
    .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .set("spark.sql.catalog.spark_catalog.type", "hive")
    .set("spark.sql.catalog.spark_catalog.uri", "thrift://220.185.154.104:9083")  // Your Hive metastore
    .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.local.type", "hadoop")
    .set("spark.sql.catalog.local.warehouse", "gs://sapient-logic-430515-v0/spark-warehouse")



  val spark = SparkSession.builder
    .master("local[*]")
    .config(conf).getOrCreate()

  // Specify the GCS bucket path where the data should be read
  val gcslandingPath = "gs://yvideos_gcp_poc/landing/USvideos.csv"
  val icebergTable = "spark_catalog.default.yvideos_vvld7" // Your Iceberg table

  // Read the DataFrame from GCS
  val df = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")
    .option("escape", "\\")
    .option("multiline", "true")
    .csv(gcslandingPath)
    .withColumn("load_date", to_date(col("trending_date"), "yy.dd.MM"))

  // Show the DataFrame (for debugging)
  df.show()

  // Write the DataFrame to the Iceberg table
  df.writeTo(icebergTable).append()  // Append to the Iceberg table

  // Optionally, you can perform any additional operations here

  spark.stop() // Stop the Spark session when done
}
