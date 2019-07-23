name := "delete_dm"

version := "1.2.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion,
  "org.apache.spark" %% "spark-catalyst" % "2.3.1"  % Test,
  "org.apache.spark" %% "spark-hive" % "2.2.0" % "provided",
  "org.apache.logging.log4j" % "log4j-api" % "2.11.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.11.0",
  "com.typesafe.play" %% "play-json" % "2.6.10",
  "com.typesafe.play" %% "play-functional" % "2.6.10",
  "com.amazonaws" % "aws-java-sdk" % "1.11.46",
  "com.google.cloud.bigtable" % "bigtable-hbase-1.x-hadoop" % "1.2.0",
  "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11",
  "com.google.cloud" % "google-cloud-storage" % "1.55.0",
  "com.google.cloud" % "google-cloud-logging-logback" % "0.77.0-alpha"
)

/*
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.6",
  "org.apache.spark" %% "spark-hive" % "2.2.0" % "provided",
  "org.apache.logging.log4j" % "log4j-api" % "2.11.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.11.0",
  "com.typesafe.play" %% "play-json" % "2.6.10",
  "com.typesafe.play" %% "play-functional" % "2.6.10",
  "com.amazonaws" % "aws-java-sdk" % "1.11.46",
  "com.google.cloud.bigtable" % "bigtable-hbase-2.x-hadoop" % "1.7.0",
  "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11",
  "com.google.cloud" % "google-cloud-storage" % "1.55.0"
)
*/