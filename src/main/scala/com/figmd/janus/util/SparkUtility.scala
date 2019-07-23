package com.figmd.janus.util

import org.apache.spark.sql.SparkSession

object SparkUtility extends Serializable {

  var appName = "cass_export"

  //var gsProjectID = prop.getProperty("gs_project_id")

  var sparkNumExecutors = "20"
  var sparkExecutorCores = "5"
  var sparkExecutorMemory = "50"
  var sparkDriverMemory = "50"
  var sparkDriverMaxresultsize = "200G"
  var sparkMasterUrl = "yarn"
  var sparkDeployMode = "cluster"


  var cassPort = "9042"

  def getSparkSession(cassandraHome: String): SparkSession = {

    var cassHostName = cassandraHome

    val spark = SparkSession.builder.master(sparkMasterUrl).appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", true)
      .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      .config("google.cloud.auth.service.account.enable", true)
      .config("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      .config("spark.yarn.maxAppAttempts", "1")
      .config("spark.memory.offHeap.enabled", true)
      .config("spark.memory.offHeap.size", "16g")
      .config("spark.sql.broadcastTimeout", "36000")
      .config("spark.network.timeout", "600s")
      .config("executor-memory", sparkExecutorMemory)
      .config("spark.submit.deployMode", sparkDeployMode)
      .config("executor-cores", sparkExecutorCores)
      .config("spark.driver.memory", sparkDriverMemory)
      .config("num-executors", sparkNumExecutors)
      .config("spark.driver.maxResultSize", sparkDriverMaxresultsize)
      .config("spark.shuffle.blockTransferService", "nio")
      .config("spark.kryoserializer.buffer.max.mb", "512")
      .config("spark.maxRemoteBlockSizeFetchToMem", "2000m")
      .config("spark.sql.hive.filesourcePartitionFileCacheSize",0)
      .config("spark.locality.wait","10s")
      .config("hive.metastore.try.direct.sql","false")
      .config("spark.cassandra.connection.host", cassHostName)
      .config("spark.cassandra.connection.port", cassPort)
      .getOrCreate()
    return spark
  }

  def getSparkSession(): SparkSession = {

    val spark = SparkSession.builder.master(sparkMasterUrl).appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", true)
      .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      .config("google.cloud.auth.service.account.enable", true)
      .config("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      .config("spark.yarn.maxAppAttempts", "1")
      .config("spark.memory.offHeap.enabled", true)
      .config("spark.memory.offHeap.size", "16g")
      .config("spark.sql.broadcastTimeout", "36000")
      .config("spark.network.timeout", "600s")
      .config("executor-memory", sparkExecutorMemory)
      .config("spark.submit.deployMode", sparkDeployMode)
      .config("executor-cores", sparkExecutorCores)
      .config("spark.driver.memory", sparkDriverMemory)
      .config("num-executors", sparkNumExecutors)
      .config("spark.driver.maxResultSize", sparkDriverMaxresultsize)
      .config("spark.shuffle.blockTransferService", "nio")
      .config("spark.kryoserializer.buffer.max.mb", "512")
      .config("spark.maxRemoteBlockSizeFetchToMem", "2000m")
      .config("spark.sql.hive.filesourcePartitionFileCacheSize",0)
      .config("spark.locality.wait","10s")
      .config("hive.metastore.try.direct.sql","false")
      .getOrCreate()
    return spark
  }




}
