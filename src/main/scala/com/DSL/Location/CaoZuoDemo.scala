package com.DSL.Location

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object CaoZuoDemo {
  def main(args: Array[String]): Unit = {
    val  Array(inputPath) = args

    val sparkSession = SparkSession.builder().master("local[*]").appName("one").getOrCreate()
    val df = sparkSession.read.parquet(inputPath)
    val a = df.groupBy("client").count()
    val b = df.select("client","bidfloor").groupBy("client").sum("bidfloor")
    val c = df.selectExpr("client","putinmodeltype").where(df("putinmodeltype").contains(1)).groupBy("client").count()
    val d = df.selectExpr("client","putinmodeltype").where(df("putinmodeltype").contains(0)).groupBy("client").count()
    val e = c.join(d,c("client")===d("client"),"outer").select(c("client"),c("count").as("open"),d("count").as("point"))
    val f = e.selectExpr("client","open","point",
      "cast(point/open*100 as decimal(20,1)) as pointlv"
    )
    val g = f.selectExpr("client","open","point","concat(pointlv,'%') as pointlv")
    val h = g.join(b ,"client").join(a,"client").select(g("client"),g("open"),g("point"),
      g("pointlv"),b("sum(bidfloor)")/a("count"))
    val i =h.select(h("client"),h("open"),h("point"),h("pointlv"),h("(sum(bidfloor) / count)").as("adPrice")).selectExpr(
      "client","open","point","pointlv","floor(adPrice) as adPrice"
    )
    val res1 = i.withColumn("isbid",lit(0)).withColumn("iswin",lit(0)).withColumn("successlv",lit(0))
    res1.show()
  }
}
