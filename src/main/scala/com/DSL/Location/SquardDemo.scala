package com.DSL.Location
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window

object SquardDemo {
  def main(args: Array[String]): Unit = {

    val  Array(inputPath) = args

    val sparkSession = SparkSession.builder().master("local[*]").appName("one").getOrCreate()
    val df = sparkSession.read.parquet(inputPath)
    val a = df.groupBy("provincename").count()
    val a1 = df.groupBy("provincename","cityname").count()
    val b = df.select("provincename","bidfloor").groupBy("provincename").sum("bidfloor")
    val c = df.selectExpr("provincename","putinmodeltype").where(df("putinmodeltype").contains(1)).groupBy("provincename").count()
    val d = df.selectExpr("provincename","putinmodeltype").where(df("putinmodeltype").contains(0)).groupBy("provincename").count()
    val b1 = df.select("provincename","cityname","bidfloor").groupBy("provincename","cityname").sum("bidfloor")
    val c1 = df.selectExpr("provincename","cityname","putinmodeltype").where(df("putinmodeltype").contains(1)).groupBy("provincename","cityname").count()
    val d1 = df.selectExpr("provincename","cityname","putinmodeltype").where(df("putinmodeltype").contains(0)).groupBy("provincename","cityname").count()
    val e = c.join(d,"provincename").select(c("provincename"),c("count").as("open"),d("count").as("point"))
    val e1 = c1.join(d1,"cityname").select(c1("provincename"),c1("cityname"),c1("count").as("open"),d1("count").as("point"))
    val f1 = e1.selectExpr("provincename","cityname","open","point",
      "cast(point/open*100 as decimal(20,1)) as pointlv"
    )
    val f = e.selectExpr("provincename","open","point",
      "cast(point/open*100 as decimal(20,1)) as pointlv"
    )
    val g1 = f1.selectExpr("provincename","cityname","open","point","concat(pointlv,'%') as pointlv")
      val g = f.selectExpr("provincename","open","point","concat(pointlv,'%') as pointlv")
    val h = g.withColumn("cityname",lit(" ")).select("provincename","cityname","open","point","pointlv")
    val i = h.join(b ,"provincename").join(a,"provincename").select(h("provincename"),h("cityname"),h("open"),h("point"),
      h("pointlv"),b("sum(bidfloor)")/a("count"))

      val j =i.select(i("provincename"),i("cityname"),i("open"),i("point"),i("pointlv"),i("(sum(bidfloor) / count)").as("adPrice")).selectExpr(
        "provincename","cityname","open","point","pointlv","floor(adPrice) as adPrice"
      )

    val res1 = j.withColumn("isbid",lit(0)).withColumn("iswin",lit(0)).withColumn("successlv",lit(0))
    val h1 = g1.join(b1 ,"cityname").join(a1,"cityname").select(g1("provincename"),g1("cityname"),g1("open"),g1("point"),
      g1("pointlv"),b1("sum(bidfloor)")/a1("count"))
    val i1 =h1.select(h1("provincename"),h1("cityname"),h1("open"),h1("point"),h1("pointlv"),h1("(sum(bidfloor) / count)").as("adPrice")).selectExpr(
      "provincename","cityname","open","point","pointlv","floor(adPrice) as adPrice"
    )
    val res2  = i1.withColumn("isbid",lit(0)).withColumn("iswin",lit(0)).withColumn("successlv",lit(0))
    val w1 =  Window.partitionBy("provincename").orderBy(asc_nulls_first("cityname"))
    val res3 = res2.union(res1)
    res3.select(res3("provincename"),res3("cityname"),res3("open"),res3("point"),res3("pointlv"),res3("adPrice"),res3("isbid"),res3("iswin").over(w1),res3("successlv"),row_number().over(w1).as("he"))
      .show()










  }
}
