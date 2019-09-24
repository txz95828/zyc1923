package com.CORE.Location

import com.CORE.util.RptUtils
import org.apache.spark.sql.SparkSession


object LoactionRpt {

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath,outputPath) =args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df = spark.read.parquet(inputPath)

    df.rdd.map(row=>{
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adordeerid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      val rptList = RptUtils.ReqPt(requestmode,processnode)
      val clickList = RptUtils.clickPt(requestmode,iseffective)
      val adList = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adordeerid,winprice,adpayment)
      val allList:List[Double] = rptList ++ clickList ++ adList
      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    })
      .map(t=>t._1+","+t._2.mkString(","))

      .saveAsTextFile(outputPath)
  }
}
