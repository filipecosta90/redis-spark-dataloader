package com.fcosta_oliveira

import java.util.UUID

import ch.qos.logback.classic.{Level, Logger}
import org.apache.spark.sql.redis.{SqlOptionLogInfoVerbose, SqlOptionModel, SqlOptionModelBlock}
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.util.Random

/*
  * @author ${user.name}
  */
object App {
  def schema(): StructType = {
    val stringColumns = (1 to 36).map(i => StructField(s"str$i", StringType)).toArray

    StructType(Array(
      StructField("id", StringType),
      StructField("int", IntegerType),
      StructField("float", FloatType),
      StructField("double", DoubleType)
    ) ++ stringColumns)
  }

  def main(args: Array[String]) {

    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).
      asInstanceOf[Logger].setLevel(Level.INFO)

    val rowsNum = 1000 * 100
    val partitionsNum = 4
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

    //val df: DataFrame = create_df(rowsNum, partitionsNum, schema _, spark )
    // used https://github.com/filipecosta90/spark-redis-datagen to generate records_rec_100000_col_400_dsize_36.parquet
    // python datagen.py --format parquet --records 100000
    val df = spark.read.parquet("data/records_rec_100000_col_400_dsize_36.parquet")


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

  private def create_df(rowsNum: Int,partitionsNum: Int, schema: () => StructType, spark: SparkSession) = {
    val rdd = spark.sparkContext.parallelize(1 to partitionsNum, partitionsNum).mapPartitions { _ =>
      def generateRow() = {
        def genStr = UUID.randomUUID().toString

        def genInt = Random.nextInt()

        def genDouble = Random.nextDouble()

        def genFloat = Random.nextFloat()

        Row.fromSeq(Seq(genStr, genInt, genFloat, genDouble) ++ (1 to 36).map(_ => genStr))
      }

      Stream.fill(rowsNum / partitionsNum)(generateRow()).iterator
    }
    val df = spark.createDataFrame(rdd, schema()).cache()
    df.count()
    df
  }

  def time[T](f: => T): T = {
    val start = System.currentTimeMillis()
    val r = f
    val end = System.currentTimeMillis()
    println(s"took ${end - start} ms")
    r
  }
}
