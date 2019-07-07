package com.fcosta_oliveira

import ch.cern.sparkmeasure.TaskMetrics
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.redis.{SqlOptionLogInfoVerbose, SqlOptionModel, SqlOptionModelBlock}
import redis.clients.jedis.Jedis

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
    val redisTimeout = 30000
    val dbnum = 0


    val jedis = new Jedis(hostip, hostport, redisTimeout)
    jedis.flushDB
    jedis.close()


    val spark = SparkSession
      .builder()
      .appName("spark-redis-profiling")
      .master("local[*]")
      .config("spark.redis.host", hostip)
      .config("spark.redis.port", hostport.toString)
      .config("spark.redis.timeout", redisTimeout)
      .config("spark.dynamicAllocation.enabled", false)
      // Class to use for serializing objects that will be sent over the network or need to be cached in serialized form.
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // Initial size of Kryo's serialization buffer, in KiB unless otherwise specified.
      // Note that there will be one buffer per core on each worker.
      .config("spark.kryoserializer.buffer", "2m")
      //enable flight recorder
      .config("spark.driver.extraJavaOptions", "-XX:+UnlockCommercialFeatures -XX:+FlightRecorder")
      .config("spark.executor.extraJavaOptions", "-XX:+UnlockCommercialFeatures -XX:+FlightRecorder")
      //enable spark measure
      .config("spark.extraListeners", "ch.cern.sparkmeasure.FlightRecorderStageMetrics")
      .config("spark.executorEnv.taskMetricsFileName", "./tmp/taskMetrics_flightRecorder.serialized")
      .config("spark.executorEnv.stageMetricsFormat", "json")
      .config("spark.sparkmeasure.printToStdout", true)
      .getOrCreate()

    val taskMetrics = TaskMetrics(spark)
    // run DataGen.scala first to generate data
    val df = spark.read.parquet("data/records_rec_100000_col_400_dsize_36_partitions_8")

    taskMetrics.runAndMeasure {

      df.write.format("org.apache.spark.sql.redis")
        .option("table", tableName)
        .option(SqlOptionModel, SqlOptionModelBlock)
        .option(SqlOptionLogInfoVerbose, true)
        .option("model.block.size", 1000)
        .option("kryoserializer.buffer.kb", "1024")
        .option("max.pipeline.size", 2500)
        .save()

    }
    taskMetrics.runAndMeasure {
      spark.read
        .format("org.apache.spark.sql.redis")
        .option("table", tableName)
        .option(SqlOptionModel, SqlOptionModelBlock)
        .option(SqlOptionLogInfoVerbose, true)
        .option("partitions.number", 12)
        .load().foreach { _ => }
    }

    //  spark.stop()

  }

}
