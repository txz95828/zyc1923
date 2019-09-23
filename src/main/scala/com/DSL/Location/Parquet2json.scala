package com.DSL.Location


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Parquet2json {

  def main(args: Array[String]): Unit = {
    // 获取目录参数
    val  Array(inputPath) = args

    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val sqlContext: SQLContext = new SQLContext(sc)
    val df: DataFrame = sqlContext.read.format("parquet").load(inputPath)
    val dd = df.rdd
    val parquetSchema = sqlContext.parquetFile(inputPath)
    val spark = SparkSession
      . builder()
      . appName("ct" )
      . master("local")
      . config("spark . serializer" ,"org. apache . spark . serializer . KryoSerializer")
      . getOrCreate()
    val file = spark. sparkContext. textFile(inputPath)

    val df1 = spark. read. parquet (inputPath )
    //注册临时视图
    df1. createTempView("log" )
    val df2 = spark
      . sql("select provincename , cityname , count(*) ct from log group by provincename , cityname " )
    df2. write.partitionBy("provincename", "cityname") . json("D:\\procity" )

    import  sqlContext.implicits._
    dd.map(x=>{
      ((x.getString(24),x.getString(25)),1)
    }).reduceByKey(_+_).map(x=>
      println("{\"ct\":"+x._2+",\"provincename\":\""+x._1._1+"\",\"cityname\":\""+x._1._2+"\"}")
    ).collect()
  }
}
