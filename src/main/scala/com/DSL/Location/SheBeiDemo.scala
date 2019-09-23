package com.DSL.Location

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object SheBeiDemo {
  def main(args: Array[String]): Unit = {
    val  Array(inputPath) = args

    val sparkSession = SparkSession.builder().master("local[*]").appName("one").getOrCreate()
    val df = sparkSession.read.parquet(inputPath)
    df.groupBy("client").count().show()

    val a = df.groupBy("devicetype").count()
    val b = df.select("devicetype","bidfloor").groupBy("devicetype").sum("bidfloor")
    val c = df.selectExpr("devicetype","putinmodeltype").where(df("putinmodeltype").contains(1)).groupBy("devicetype").count()
    val d = df.selectExpr("devicetype","putinmodeltype").where(df("putinmodeltype").contains(0)).groupBy("devicetype").count()
    val e = c.join(d,c("devicetype")===d("devicetype"),"leftouter").select(c("devicetype"),c("count").as("open"),d("count").as("point"))
    val f = e.selectExpr("devicetype","open","point",
      "cast(point/open*100 as decimal(20,1)) as pointlv"
    )
    val g = f.selectExpr("devicetype","open","point","concat(pointlv,'%') as pointlv")
    val h = g.join(b ,"devicetype").join(a,"devicetype").select(g("devicetype"),g("open"),g("point"),
      g("pointlv"),b("sum(bidfloor)")/a("count"))
    val i =h.select(h("devicetype"),h("open"),h("point"),h("pointlv"),h("(sum(bidfloor) / count)").as("adPrice")).selectExpr(
      "devicetype","open","point","pointlv","floor(adPrice) as adPrice"
    )
    val res1 = i.withColumn("isbid",lit(0)).withColumn("iswin",lit(0)).withColumn("successlv",lit(0))
    res1.show()
  }
}
