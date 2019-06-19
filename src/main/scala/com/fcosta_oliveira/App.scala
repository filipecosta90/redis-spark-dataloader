package com.fcosta_oliveira

import org.apache.spark.sql.SparkSession


/**
  * @author ${user.name}
  */
object App {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("myApp")
      .master("local[*]")
      .config("spark.redis.host", "localhost")
      .config("spark.redis.port", "6379")
      .getOrCreate()

    val sc = spark.sparkContext

  }

}
