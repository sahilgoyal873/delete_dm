package com.figmd.janus.util

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{DataFrame, SparkSession}

object PostgreUtility extends Serializable {

  var wf_id=GCSUtility.prop.getProperty("wf_id")

  def postgresConnect(reg: String): Connection ={

    val url = "jdbc:postgresql://"+GCSUtility.prop.getProperty("postgresHostName")+":"+GCSUtility.prop.getProperty("postgresHostPort")+"/"+reg +"_management?tcpKeepAlive=true&socketTimeout=0&connectTimeout=0"
    val driver = "org.postgresql.Driver"
    val username = GCSUtility.prop.getProperty("postgresHostUserName")
    val password = GCSUtility.prop.getProperty("postgresUserPass")

    Class.forName(driver)
    return DriverManager.getConnection(url, username, password)
 }

  def postgresConnectLookup(reg: String): Connection ={

    val url = "jdbc:postgresql://"+GCSUtility.prop.getProperty("postgresHostName")+":"+GCSUtility.prop.getProperty("postgresHostPort")+"/"+reg +"_lookup?tcpKeepAlive=true&socketTimeout=0&connectTimeout=0"
    val driver = "org.postgresql.Driver"
    val username = GCSUtility.prop.getProperty("postgresHostUserName")
    val password = GCSUtility.prop.getProperty("postgresUserPass")

    Class.forName(driver)
    return DriverManager.getConnection(url, username, password)
  }

  def insertIntoProcessDetails(con: Connection, practice_id: Int, end_date: Timestamp): Unit = {
    try {
      val pstatement = con.prepareStatement("update practicerefreshstatus set enddate=? , lastrefreshtime=now()  where practiceid=?")
      pstatement.setTimestamp(1, end_date)
      pstatement.setInt(2, practice_id)

      println(pstatement.toString)

      pstatement.executeUpdate()
    }
    catch {
      case e: Exception => println("\npostgres practicerefreshstatus connection ERROR..." + e.printStackTrace())
    }
  }


  def getElementsFromPostgres(spark: SparkSession, registry: String): String = {
    val connection: Connection = postgresConnectLookup(registry)
    try {



      val pstatement = connection.prepareStatement("select array_agg(distinct lower(shortname)) from elementmaster")

      println(pstatement.toString)

      val result = pstatement.executeQuery()

      println("resultset size : "+result.getFetchSize)

      if(result.next){
        result.getArray(1).toString
      }else "NULL"

    }
    catch {
      case e: Exception => println("\npostgres process_details_stg connection ERROR..." + e.printStackTrace()) ; return "NULL"
    }
    finally {
      connection.close()
    }
  }

  def updatePostgresdates(spark: SparkSession, registry: String, cleanDF: DataFrame): Unit = {

    val connection: Connection = postgresConnect(registry)
    try {

      val out = cleanDF.select("practice_id", "visit_dt").distinct().groupBy("practice_id").agg(max("visit_dt")).collect()

      out.foreach(r => {

        println(r.toString())

        insertIntoProcessDetails(connection, r.getInt(0), r.getTimestamp(1))
      }
      )
    }
    catch {
      case e: Exception => println("\npostgres process_details_stg connection ERROR..." + e.printStackTrace())
    }
    finally {
      connection.close()
    }
  }


}
