package com.figmd.janus.util

import java.io.File
import java.util.Properties

import com.amazonaws.{AmazonClientException, AmazonServiceException}
import org.apache.hadoop.conf.Configuration

object GCSUtility {

  var prop = new Properties
  val hadoopConfiguration: Configuration = new Configuration()

  def readS3ConfigFile(args: Array[String]): Unit = {


    prop.setProperty("postgresHostName", "10.42.0.5")
    prop.setProperty("postgresHostPort", "5432")
    prop.setProperty("postgresHostUserName", "postgres")
    prop.setProperty("postgresUserPass", "figmd@123")

/*
    val FILE_PATH = args(0)

    try {
      var fileData = GCSFileUtility.readFile(hadoopConfiguration, FILE_PATH)
      for (property <- fileData) {
        if(!property.startsWith("#")) {
          val sp = property.split("=")
          val key = sp(0)
          val value = sp(1)
          prop.setProperty(key, value)
          println(key+"="+value)
        }
      }
      println("......................................................................................................................")

    } catch {
      //TODO: Clean up
      case ase: AmazonServiceException => System.err.println("Exception: " + ase.toString)
      case ace: AmazonClientException => System.err.println("Exception: " + ace.toString)
      case e: Exception => println("Unable to read Config file"+ e.printStackTrace())
    }*/

  }
}
