package com.DSL.Location

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object PathDemo {
  def main(args: Array[String]): Unit = {
    val  Array(inputPath) = args

    val sparkSession = SparkSession.builder().master("local[*]").appName("one").getOrCreate()
    val df = sparkSession.read.parquet(inputPath)
    val a = df.rollup("mediatype").count()
    val b = df.select("mediatype","bidfloor").groupBy("mediatype").sum("bidfloor")
    val c = df.selectExpr("mediatype","putinmodeltype").where(df("putinmodeltype").contains(1)).groupBy("mediatype").count()
    val d = df.selectExpr("mediatype","putinmodeltype").where(df("putinmodeltype").contains(0)).groupBy("mediatype").count()
    val e = c.join(d,c("mediatype")===d("mediatype"),"outer").select(c("mediatype"),c("count").as("open"),d("count").as("point"))
    val f = e.selectExpr("mediatype","open","point",
      "cast(point/open*100 as decimal(20,1)) as pointlv"
    )
    val g = f.selectExpr("mediatype","open","point","concat(pointlv,'%') as pointlv")
    val h = g.join(b ,"mediatype").join(a,"mediatype").select(g("mediatype"),g("open"),g("point"),
      g("pointlv"),b("sum(bidfloor)")/a("count"))
    val i =h.select(h("mediatype"),h("open"),h("point"),h("pointlv"),h("(sum(bidfloor) / count)").as("adPrice")).selectExpr(
      "mediatype","open","point","pointlv","floor(adPrice) as adPrice"
    )
    val res1 = i.withColumn("isbid",lit(0)).withColumn("iswin",lit(0)).withColumn("successlv",lit(0))
    res1.show()
    df.groupBy("cityname").pivot("provincename").pivot("appname").count().show()

  }
}
