package com.fcosta_oliveira

import java.util.UUID

import ch.qos.logback.classic.{Level, Logger}
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.Random

/*
  * @author ${user.name}
  */
object App {


  def main(args: Array[String]) {

    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).
      asInstanceOf[Logger].setLevel(Level.INFO)

    val rowsNum = 1000 * 1000
    val partitionsNum = 4
    val tableName = "1M"

    def schema(): StructType = {
      val stringColumns = (1 to 36).map(i => StructField(s"str$i", StringType)).toArray

      StructType(Array(
        StructField("id", StringType),
        StructField("int", IntegerType),
        StructField("float", FloatType),
        StructField("double", DoubleType)
      ) ++ stringColumns)
    }

    def time[T](f: => T): T = {
      val start = System.currentTimeMillis()
      val r = f
      val end = System.currentTimeMillis()
      println(s"took ${end - start} ms")
      r

    }
    // Distribute a local Scala collection to form an RDD.

    val spark = SparkSession
      .builder()
      .appName("spark-redis-profiling")
      .master("local[*]")
      .config("spark.redis.host", "localhost")
      .config("spark.redis.port", "6379")
      .config("spark.dynamicAllocation.enabled", false)
      .getOrCreate()

    //val df: DataFrame = create_df(rowsNum, partitionsNum, schema _, spark )
    val df = spark.read.parquet("data/records_rec_100000_col_400_dsize_36.parquet")

    time {
      df.write.format("org.apache.spark.sql.redis").option("table", tableName).save()
    }
    time {
      spark.read
        .format("org.apache.spark.sql.redis")
        .option("table", tableName)
        .load().count()
    }
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
