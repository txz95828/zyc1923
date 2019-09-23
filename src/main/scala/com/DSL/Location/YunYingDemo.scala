package com.DSL.Location

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object YunYingDemo {
  def main(args: Array[String]): Unit = {
    val  Array(inputPath) = args

    val sparkSession = SparkSession.builder().master("local[*]").appName("one").getOrCreate()
    val df = sparkSession.read.parquet(inputPath)


    val a = df.groupBy("ispname").count()
    val b = df.select("ispname","bidfloor").groupBy("ispname").sum("bidfloor")
    val c = df.selectExpr("ispname","putinmodeltype").where(df("putinmodeltype").contains(1)).groupBy("ispname").count()
    val d = df.selectExpr("ispname","putinmodeltype").where(df("putinmodeltype").contains(0)).groupBy("ispname").count()
    val e = c.join(d,c("ispname")===d("ispname"),"leftouter").select(c("ispname"),c("count").as("open"),d("count").as("point"))
    val f = e.selectExpr("ispname","open","point",
      "cast(point/open*100 as decimal(20,1)) as pointlv"
    )
    val g = f.selectExpr("ispname","open","point","concat(pointlv,'%') as pointlv")
    val h = g.join(b ,"ispname").join(a,"ispname").select(g("ispname"),g("open"),g("point"),
      g("pointlv"),b("sum(bidfloor)")/a("count"))
    val i =h.select(h("ispname"),h("open"),h("point"),h("pointlv"),h("(sum(bidfloor) / count)").as("adPrice")).selectExpr(
      "ispname","open","point","pointlv","floor(adPrice) as adPrice"
    )
    val res1 = i.withColumn("isbid",lit(0)).withColumn("iswin",lit(0)).withColumn("successlv",lit(0))
    res1.show()

  }
}
