package com.fcosta_oliveira

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.redis.{SqlOptionLogInfoVerbose, SqlOptionModel, SqlOptionModelBlock}
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/*
  * @author ${user.name}
  */
object App {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("akka").setLevel(Level.INFO)

    val tableName = "100K"

    // Distribute a local Scala collection to form an RDD.

    val hostip = "localhost"
    val hostport = 6379
    val dbnum = 0

    val pool = new JedisPool(new JedisPoolConfig, hostip, hostport, 5000)
    val jedis = pool.getResource
    jedis.flushDB
    jedis.close()

    val spark = SparkSession
      .builder()
      .appName("spark-redis-profiling")
      .master("local[*]")
      .config("spark.redis.host", hostip)
      .config("spark.redis.port", hostport.toString)
      .config("spark.redis.kryoserializer.buffer.mb","32")
      .config("spark.dynamicAllocation.enabled", false)
      // Class to use for serializing objects that will be sent over the network or need to be cached in serialized form.
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      // Initial size of Kryo's serialization buffer, in KiB unless otherwise specified.
      // Note that there will be one buffer per core on each worker.
      .config("spark.kryoserializer.buffer","2m")
      .getOrCreate()

    // run DataGen.scala first to generate data
    val df = spark.read.parquet("data/records_rec_100000_col_400_dsize_36")

    time {
      df.write.format("org.apache.spark.sql.redis")
        .option("table", tableName)
        .option(SqlOptionModel, SqlOptionModelBlock)
        .option(SqlOptionLogInfoVerbose, true)
        .save()
    }
//
//    time {
//      spark.read
//        .format("org.apache.spark.sql.redis")
//        .option("table", tableName)
//        .option(SqlOptionModel, SqlOptionModelBlock)
//        .option(SqlOptionLogInfoVerbose, true)
//        .load().foreach { _ => }
//    }
  }

  def time[T](f: => T): T = {
    val start = System.currentTimeMillis()
    val r = f
    val end = System.currentTimeMillis()
    println(s"took ${end - start} ms")
    r
  }
}
