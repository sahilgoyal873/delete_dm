package com.figmd.janus.util

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object GCSFileUtility {

  def readFile(hadoopConf: Configuration, fileName: String): Array[String] = {
    val inputFilePath = new Path(fileName)
    val hadoopFileSystem = inputFilePath.getFileSystem(hadoopConf)
    val br: BufferedReader = new BufferedReader(new InputStreamReader(hadoopFileSystem.open(inputFilePath)))
    val data = br.lines().toArray
    br.close()
    var outputData1 = data.toStream.map(line => line.toString).toArray
    return outputData1
  }
}
