package com.imaginea.spark.core

import java.io.{File, BufferedWriter, FileInputStream, OutputStreamWriter}
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.sys.process._
/**
  * Created by sampathr on 23/8/16.
  */
object CsvToParquetConverter {

  def main(args: Array[String]) {
    writeToHdfs("sample-s3.log")
  }

  def writeToHdfs(fileName: String) = {
    val outputFile = s"hdfs://localhost:54310/user/data/test/$fileName"
    print(outputFile)
    val outputPath = new Path(outputFile)
    val config = new Configuration()
    config.set("dfs.replication", "1")
    val fs = FileSystem.get(URI.create(outputFile), config)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath, true)))
    //val is = Source.fromInputStream(new FileInputStream("hdfs://localhost:54310/user/data/sample-s3.log"))
    val is = Source.fromInputStream(new FileInputStream("data/sample-s3.log"))
    val lines = is.getLines()
    val startOfNetTime = System.currentTimeMillis()
    var writeTime = 0L
    lines.drop(3) foreach { line =>
      val deviceValue = line.split("\\t")(93)
      if (!(deviceValue == "-" || deviceValue == "" || deviceValue.equalsIgnoreCase("nan"))) {
        val startOfWriteTimePerRecord = System.currentTimeMillis()
        val lineData = line

        writer.write(line)
        writer.write("\n")
        val writeTimePerRecord = System.currentTimeMillis() - startOfWriteTimePerRecord
        writeTime += writeTimePerRecord
      }
    }
    val netTime = System.currentTimeMillis() - startOfNetTime
    print(netTime)
    writer.close()
    is.close()
  }

}
