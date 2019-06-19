package com.fcosta_oliveira


import java.io.{File, FileWriter, PrintWriter}
import java.time.{Duration => JDuration}

import org.apache.spark.sql.SparkSession


/**
  * @author ${user.name}
  */
object App {

  val benchmarkReportDir = new File("target/reports/benchmarks/")

  def main(args: Array[String]) {

    val recordName = "data/10K_records_100cols_36_bytes.csv"
    val tableName = "10K_records_100cols_36_bytes"
    benchmarkReportDir.mkdirs()


    val spark = SparkSession
      .builder()
      .appName("myApp")
      .master("local[*]")
      .config("spark.redis.host", "localhost")
      .config("spark.redis.port", "6379")
      .getOrCreate()

    //load

    redis_load(recordName, spark)

    //write
    redis_write(recordName, tableName, spark)


    //read after write
    redis_read(recordName, tableName, spark)

  }

  private def redis_read(recordName: String, tableName: String, spark: SparkSession) = {
    val df = spark.read.format("csv").option("header", "true").load(recordName)
    time(s"Read") {
      spark.read
        .format("org.apache.spark.sql.redis")
        .option("table", tableName)
        .load()
    }
  }

  private def redis_load(recordName: String, spark: SparkSession) = {
    time(s"Load") {
      val df = spark.read.format("csv").option("header", "true").load(recordName)
    }
  }

  private def redis_write(recordName: String, tableName: String, spark: SparkSession) = {
    val df = spark.read.format("csv").option("header", "true").load(recordName)
    time(s"Write") {
      df.write
        .format("org.apache.spark.sql.redis")
        .option("table", tableName)
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .save()
    }
  }

  def time[R](tag: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    new PrintWriter(new FileWriter(s"$benchmarkReportDir/results.txt", true)) {
      // scalastyle:off
      //Tag,  Duration representing a number of nanoseconds.
      this.println(s"$tag, ${JDuration.ofNanos(t1 - t0)}")
      close()
    }
    result
  }
}
