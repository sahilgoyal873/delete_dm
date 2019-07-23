package com.figmd.janus

import java.util.Calendar
import java.util.logging.{Level, Logger}

import com.figmd.janus.util.{GCSUtility, PostgreUtility, SparkUtility}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object delete_dm {

  var cassHostName = ""
  var inDir = ""

  def main(args: Array[String]) {
    try {

      val start_date = Calendar.getInstance.getTime

      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      Logger.getLogger("spark-log4j").setLevel(Level.OFF)

      println("[" + Calendar.getInstance().getTime() + "] " + "JOB STARTED")

      val reg_name = args(0)
      inDir = args(1)
      cassHostName = args(2)
      var element_list = args(3)
      val practiceid = args(4)

      val spark = SparkUtility.getSparkSession(cassHostName)

      GCSUtility.readS3ConfigFile(args)

      if(element_list == "NA") element_list = PostgreUtility.getElementsFromPostgres(spark,reg_name.toLowerCase).toLowerCase

      println("Registry : " + reg_name)
      println("Deleting Elements : " + element_list)
      println("Deleting Practices : " + practiceid)

      //deleteFromTbl(spark, reg_name, "tblencounter_2019", element_list, practiceid)
      deleteFromPH(spark, reg_name, "patient_history_2019", element_list, practiceid)


    }
    catch {
      case e: Exception => {
        println("[" + Calendar.getInstance().getTime() + "] " + e.printStackTrace())
        System.exit(-1)
      }
    }
  }

  def deleteFromTbl(spark: SparkSession, registry: String, tableName: String, element_list: String, practiceList: String): Unit = {

    println("\n\n---------------------------------  Starting table : " + tableName + "----------------------------------------\n\n")

    val prod_keyspace = registry.toLowerCase + "_dm_4"
    val prod_bck_save_path = registry.toLowerCase + "-janus-deployment/cassandra/backup/clean_datamart/" + inDir


    var elements1 = element_list.toLowerCase.split(",")


    var practices1 = practiceList.split(",")

    val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> tableName, "keyspace" -> prod_keyspace))
      .load().select("visituid", "encounterdate", "practiceid", "providerid")
      .filter(col("practiceid").isin(practices1: _*))

    df.createOrReplaceTempView(tableName)

    println("Full count of " + tableName + "        : " + df.count())


    df.write.mode(SaveMode.Overwrite).option("header", "true").option("delimiter", "~").csv(s"gs://$prod_bck_save_path/$tableName/")
    var df_new = df

    var elements = element_list.split(",")
    var practices = practiceList.split(",")

    elements.foreach(r => {
      var element = r.toLowerCase
      df_new = df_new.withColumn(element, lit(null))
        .withColumn(element + "_date", lit(null))
    }

    )


    println("Full count after deletion from " + tableName + " : " + df_new.count())

    loadCleanDataAppend(spark, registry, tableName, df_new)

    println("\n\n---------------------------------  End table : " + tableName + "----------------------------------------\n\n")
  }

  def loadCleanDataAppend(spark: SparkSession, registry: String, tableName: String, cleanDF: DataFrame): Unit = {

    val cleanKeyspace = registry.toLowerCase + "_dm_4"

    cleanDF.write.mode(SaveMode.Append)
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> cleanKeyspace))
      .save()


  }

  def deleteFromPH(spark: SparkSession, registry: String, tableName: String, element_list: String, practiceList: String): Unit = {

    println("\n\n---------------------------------  Starting table : " + tableName + "----------------------------------------\n\n")

    val prod_keyspace = registry.toLowerCase + "_dm_4"
    val prod_bck_save_path = registry.toLowerCase + "-janus-deployment/cassandra/backup/clean_datamart/" + inDir

    val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> tableName, "keyspace" -> prod_keyspace, "read_request_timeout_in_ms" -> "200000"))
      .load().repartition(2005)

    df.cache()

    println("Full count of " + tableName + "        : " + df.count())

    df.write.mode(SaveMode.Overwrite).option("header", "true").option("delimiter", "~").csv(s"gs://$prod_bck_save_path/$tableName/")
    var df_new = df

    var elements = element_list.toLowerCase.split(",")
    var practices = practiceList.split(",")

    df_new = df_new.filter(!(col("practiceid").isin(practices: _*) && col("element").isin(elements: _*)))


    println("Full count after deletion from " + tableName + " : " + df_new.count())

    loadCleanData(spark, registry, tableName, df_new)

    println("\n\n---------------------------------  End table : " + tableName + "----------------------------------------\n\n")
  }

  def loadCleanData(spark: SparkSession, registry: String, tableName: String, cleanDF: DataFrame): Unit = {

    val cleanKeyspace = registry.toLowerCase + "_dm_4"

    cleanDF.write.mode(SaveMode.Overwrite)
      .option("confirm.truncate", "true")
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> cleanKeyspace))
      .save()


  }


}
