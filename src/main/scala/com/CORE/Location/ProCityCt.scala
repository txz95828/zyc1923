package com.CORE.Location

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object ProCityCt {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    if(args.length != 1){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath) =args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df = spark.read.parquet(inputPath)
    df.createTempView("log")
    val df2 = spark
      .sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")

    df2.write.partitionBy("provincename","cityname").json("D:\\procity")


    spark.stop()
  }
}