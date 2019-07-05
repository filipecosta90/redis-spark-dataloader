package com.fcosta_oliveira

import java.util.UUID

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.Random


object DataGen {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val rowsNum = 1000 * 100
    val dataSize = 36
    val columns = 400
    val partitionsNum = 12 // only affect the number of cores used (make sure that rowsNum % partitionsNum = 0)
    val outputFile = s"records_rec_${rowsNum}_col_${columns}_dsize_${dataSize}_partitions_${partitionsNum}"
    val outputPath = "data/"

    val spark = SparkSession
      .builder()
      .appName("data-gen")
      .master("local[*]")
      .getOrCreate()

    val df = createDf(spark, rowsNum, dataSize, columns, partitionsNum)
//    df.cache()
//    df.show(1)
//    println(s"Generated ${df.count()} records")
    df.write.mode(SaveMode.Overwrite).parquet(outputPath + outputFile)
  }

  private def createDf(spark: SparkSession, rowsNum: Int, dataSize: Int, columns: Int, partitionsNum: Int) = {
    val schema = {
      val stringColumns = (1 to columns).map(i => StructField(s"str$i", StringType))
      StructType(stringColumns)
    }

    val rdd = spark.sparkContext.parallelize(1 to partitionsNum, partitionsNum).mapPartitions { _ =>
      def generateRow() = {
        def genStr = Random.alphanumeric.take(dataSize).mkString("")

        Row.fromSeq(Seq.fill(columns)(genStr))
      }

      Stream.fill(rowsNum / partitionsNum)(generateRow()).iterator
    }
    spark.createDataFrame(rdd, schema)
  }

}
