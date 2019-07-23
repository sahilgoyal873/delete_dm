package com.figmd.janus

import com.datastax.driver.core.{Cluster, ConsistencyLevel, QueryOptions, SocketOptions}
import java.util.Calendar
import java.util.logging.{Level, Logger}

import com.figmd.janus.util.{GCSUtility, SparkUtility}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}



object practice_delete {


  var cassHostName = ""
  var inDir = Calendar.getInstance.getTime.toInstant.toString

  def main(args: Array[String]) {
    try {

      val start_date = Calendar.getInstance.getTime

      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      Logger.getLogger("spark-log4j").setLevel(Level.OFF)

      println("[" + Calendar.getInstance().getTime() + "] " + "JOB STARTED")

      val reg_name = args(0)
      cassHostName = args(1)
      val practiceid = args(2)

      val spark = SparkUtility.getSparkSession(cassHostName)

      GCSUtility.readS3ConfigFile(args)

      println("Registry           : " + reg_name)
      println("Deleting Practices : " + practiceid)

      deleteData(spark, reg_name, "tblencounter_2019", practiceid)
      deleteData(spark, reg_name, "patient_history_2019", practiceid)


    }
    catch {
      case e: Exception => {
        println("[" + Calendar.getInstance().getTime() + "] " + e.printStackTrace())
        System.exit(-1)
      }
    }
  }


  def deleteData(spark: SparkSession, registry: String, tableName: String, practiceList: String): Unit = {

    println("\n\n---------------------------------  Starting table : " + tableName + "----------------------------------------\n\n")

    val prod_keyspace = registry.toLowerCase + "_dm_4"
    val prod_bck_save_path = registry.toLowerCase + "-janus-deployment/cassandra/backup/datamart_delete/" + inDir

    var practices = practiceList.split(",")

    val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> tableName, "keyspace" -> prod_keyspace))
      .load().repartition(col("practiceid"))
      .filter(col("practiceid").isin(practices: _*))

    df.createOrReplaceTempView(tableName)
    df.cache()

    println("count of " + tableName + "        : " + df.count())

    df.write.partitionBy("practiceid").mode(SaveMode.Overwrite).option("header", "true").option("delimiter", "~").csv(s"gs://$prod_bck_save_path/$tableName/")

    practices.foreach(
      practiceid=> {
        val query = s"delete from $prod_keyspace.$tableName where practiceid=$practiceid"
        executeCassandraQuery(prod_keyspace, query)
      }
    )

    println("\n\n---------------------------------  End table : " + tableName + "----------------------------------------\n\n")
  }


  def executeCassandraQuery(keyspace: String, query: String): Unit = {
    val cluster = Cluster.builder
      .addContactPoint(cassHostName)
      .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(60000000))
      .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE))
      .build
    val session = cluster.connect(keyspace)

    System.out.println("Cassandra Query Executed  :::" + query)

    if (query != "")
      session.execute(query)
    else
      null

    session.close()
    cluster.close()
  }





}
