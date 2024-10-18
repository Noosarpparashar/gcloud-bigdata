ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"
val sparkVersion = "3.5.1"

resolvers += "jitpack" at "https://jitpack.io"

val dependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion excludeAll (
    ExclusionRule("org.apache.hadoop"),
    ExclusionRule("com.google.guava", "guava") // Exclude Guava here
  ),
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.22" exclude("com.google.guava", "guava"),
  "com.google.guava" % "guava" % "33.1.0-jre",
  "org.postgresql" % "postgresql" % "42.2.24",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.0",
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.6.1",
  "org.apache.hive" % "hive-exec" % "3.1.3",
  "org.apache.hadoop" % "hadoop-common" % "3.3.6",

)
dependencyOverrides += "com.github.luben" % "zstd-jni" % "1.5.6-4"



resolvers += "Confluent" at "https://packages.confluent.io/maven/"
libraryDependencies ++= dependencies

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val root = (project in file("."))
  .settings(
    name := "videos-gcp-emr"
  )