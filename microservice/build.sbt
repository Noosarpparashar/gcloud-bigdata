ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"
val sparkVersion = "3.5.1"

resolvers += "jitpack" at "https://jitpack.io"

val dependencies = Seq(
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "com.google.guava" % "guava" % "33.1.0-jre",
  "com.typesafe.akka" %% "akka-http" % "10.2.7",
  "com.typesafe.akka" %% "akka-stream" % "2.7.0",
  "com.typesafe.slick" %% "slick" % "3.3.3",
  "org.postgresql" % "postgresql" % "42.2.24",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.3.3",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.5.3",
  "org.postgresql" % "postgresql" % "42.6.0",


)
dependencyOverrides += "com.github.luben" % "zstd-jni" % "1.5.6-4"


resolvers += "Confluent" at "https://packages.confluent.io/maven/"
libraryDependencies ++= dependencies

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val root = (project in file("."))
  .settings(
    name := "videos-gcp-emr"
  )
