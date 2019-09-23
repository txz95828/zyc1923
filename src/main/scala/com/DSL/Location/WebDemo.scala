package com.DSL.Location

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object WebDemo {
  def main(args: Array[String]): Unit = {
    val  Array(inputPath) = args

    val sparkSession = SparkSession.builder().master("local[*]").appName("one").getOrCreate()
    val df = sparkSession.read.parquet(inputPath)
    df.groupBy("networkmannername").count().show()
    val a = df.groupBy("networkmannername").count()
    val b = df.select("networkmannername","bidfloor").groupBy("networkmannername").sum("bidfloor")
    val c = df.selectExpr("networkmannername","putinmodeltype").where(df("putinmodeltype").contains(1)).groupBy("networkmannername").count()
    val d = df.selectExpr("networkmannername","putinmodeltype").where(df("putinmodeltype").contains(0)).groupBy("networkmannername").count()
    val e = c.join(d,c("networkmannername")===d("networkmannername"),"leftouter").select(c("networkmannername"),c("count").as("open"),d("count").as("point"))
    val f = e.selectExpr("networkmannername","open","point",
      "cast(point/open*100 as decimal(20,1)) as pointlv"
    )
    val g = f.selectExpr("networkmannername","open","point","concat(pointlv,'%') as pointlv")
    val h = g.join(b ,"networkmannername").join(a,"networkmannername").select(g("networkmannername"),g("open"),g("point"),
      g("pointlv"),b("sum(bidfloor)")/a("count"))
    val i =h.select(h("networkmannername"),h("open"),h("point"),h("pointlv"),h("(sum(bidfloor) / count)").as("adPrice")).selectExpr(
      "networkmannername","open","point","pointlv","floor(adPrice) as adPrice"
    )
    val res1 = i.withColumn("isbid",lit(0)).withColumn("iswin",lit(0)).withColumn("successlv",lit(0))
    res1.show()





  }
}
