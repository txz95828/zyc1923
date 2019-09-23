package com.DSL.Location

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object MovieDemo {
  def main(args: Array[String]): Unit = {
    val  Array(inputPath) = args

    val sparkSession = SparkSession.builder().master("local[*]").appName("one").getOrCreate()
    val df = sparkSession.read.parquet(inputPath)
    val a = df.groupBy("appname").count()
    val b = df.select("appname","bidfloor").groupBy("appname").sum("bidfloor")
    val c = df.selectExpr("appname","putinmodeltype").where(df("putinmodeltype").contains(1)).groupBy("appname").count()
    val d = df.selectExpr("appname","putinmodeltype").where(df("putinmodeltype").contains(0)).groupBy("appname").count()
    val e = c.join(d,c("appname")===d("appname"),"outer").select(c("appname"),c("count").as("open"),d("count").as("point"))
    val f = e.selectExpr("appname","open","point",
      "cast(point/open*100 as decimal(20,1)) as pointlv"
    )
    val g = f.selectExpr("appname","open","point","concat(pointlv,'%') as pointlv")
    val h = g.join(b ,"appname").join(a,"appname").select(g("appname"),g("open"),g("point"),
      g("pointlv"),b("sum(bidfloor)")/a("count"))
    val i =h.select(h("appname"),h("open"),h("point"),h("pointlv"),h("(sum(bidfloor) / count)").as("adPrice")).selectExpr(
      "appname","open","point","pointlv","floor(adPrice) as adPrice"
    )
    val res1 = i.withColumn("isbid",lit(0)).withColumn("iswin",lit(0)).withColumn("successlv",lit(0))
    res1.show()
  }
}
