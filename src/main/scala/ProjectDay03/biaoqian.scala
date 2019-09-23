package ProjectDay03



import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


object biaoqian {
  def main(args: Array[String]): Unit = {
    val Array(inputPath) = args

    val sparkSession = SparkSession.builder().master("local[*]").appName("one").getOrCreate()
    val df = sparkSession.read.parquet(inputPath)
    val w1 =Window.orderBy("userid")
    val w2 = Window.orderBy("value")
    val w3 = Window.orderBy("id")
    val dv = df.withColumn("id",row_number().over(w1))
    val a = dv.groupBy("userid", "adspacetype", "adspacetypename").count()
    val b = a.select("userid", "adspacetype", "adspacetypename", "count")
    val c = b.select(b("userid"), lpad(b("adspacetype"), 2, "0").as("adspacetype"), b("adspacetypename"), b("count").as("num"))
    val d = c.selectExpr("userid", "concat('LN',adspacetypename) as adspacetypename", "concat('LC',adspacetype) as adspacetype","num").where(c("userid").notEqual(""))
    val e = d.selectExpr("concat(adspacetype,' -> ',num) as adspacetype", "concat(adspacetypename,' -> ',num) as adspacetypename").where(d("userid").notEqual(""))
    val f = e.groupBy("adspacetype", "adspacetypename").count()
    val res1 = f.selectExpr("adspacetype", "adspacetypename")
   // res1.show()

    val a1 = df.groupBy("userid", "appname").count()
    val b1 = a1.select(a1("userid"), a1("appname"), a1("count").as("num")).where(a1("userid").notEqual(""))
    val c1 = b1.selectExpr("userid", "concat('APP',appname) as appname","num").where(b1("userid").notEqual(""))
    val d1 = b1.selectExpr("concat('APP',appname,' -> ',num) as appname").where(b1("userid").notEqual(""))
    val e1 = d1.groupBy("appname").count()
    val res2 = e1.selectExpr("appname")
   // res2.show()

    val a2 = df.groupBy("userid", "adplatformproviderid").count()
    val b2 = a2.select(a2("userid"), a2("adplatformproviderid"),a2("count").as("num")).where(a2("userid").notEqual(""))
    val c2 = b2.selectExpr("userid", "concat('CN',adplatformproviderid) as adplatformproviderid","num").where(b2("userid").notEqual(""))
    val d2 = b2.selectExpr("concat('CN',adplatformproviderid,' -> ',num) as adplatformproviderid").where(b2("userid").notEqual(""))
    val e2 = d2.groupBy("adplatformproviderid").count()
    val res3 = e2.selectExpr("adplatformproviderid")
    //res3.show()

    val a3 = df.groupBy("userid", "client").count()
    val b3 = a3.selectExpr("userid", "(case when client = 1 then '1 Android D00010001' when client = 2 then '2 IOS D00010002' when client = 3 then '3 WinPhone D00010003' when client = 4 then '_ 其 他 D00010004' end) as client")
      .where(a3("userid").notEqual(""))
    val c3 = b3.groupBy("client").count()
    val res4 = c3.selectExpr("client")
   // res4.show()

    val a4 = df.groupBy("userid", "networkmannername").count()
    val b4 = a4.selectExpr("userid", "(case when networkmannername = '2G' then '2G D00020004' when networkmannername = '3G' then '3G D00020003' when networkmannername = '4G' then '4G D00020002' when networkmannername = 'Wifi' then 'WIFI D00020001 ' else  '_   D00020005' end) as networkmannername")
      .where(a4("userid").notEqual(""))
    val c4 = b4.groupBy("networkmannername").count()
    val res5 = c4.selectExpr("networkmannername")
    //res5.show()

    val a5 = df.groupBy("userid", "ispname").count()
    val b5 = a5.selectExpr("userid", "(case when ispname = '移动' then '移 动 D00030001'when ispname = '联通' then '联 通 D00030002 ' when ispname = '电信' then '电 信 D00030003' else '_ D00030004' end) as ispname")
      .where(a5("userid").notEqual(""))
    val c5 = b5.groupBy("ispname").count()
    val res6 = c5.selectExpr("ispname")
    //res6.show()

    val a6 = df.groupBy("userid", "keywords").count()
    val b6 = a6.select(a6("userid"), explode(split(a6("keywords"), "\\|")).as("keywords")).where(a6("userid").notEqual(""))
    val c6 = b6.groupBy("userid", "keywords").count()
    val re = c6.groupBy("userid").count()
    val re1 = re.select(re("userid"), re("count").as("num"))
    val re2 = re1.join(c6, "userid")
    val re3 = re2.select("userid", "keywords", "count").where(re2("num") >= 3 and (re2("num") <= 8))
    val d6 = re3.selectExpr("userid", "concat(keywords,':',count) as keywords").where(c6("keywords").notEqual(""))
    val e6 = d6.groupBy("userid").agg(collect_set(d6("keywords")))
    val f6 = e6.select(e6("userid"), e6("collect_set(keywords)").as("keywords"))
    val g6 = f6.select(f6("userid"), concat_ws(",", f6("keywords")).as("keywords"))
    val h6 = g6.selectExpr("userid", "concat('K ',keywords) as keywords")

    val a7 = df.groupBy("userid", "provincename", "cityname").count()
    val b7 = a7.select("userid", "provincename", "cityname","count")
    val c7 = b7.selectExpr("userid", "concat('ZP',provincename) as provincename", "concat('ZC',cityname) as cityname","count").where(b7("userid").notEqual(""))
    val d7 = c7.selectExpr("concat(provincename,' -> ',count) as provincename", "concat(cityname,' -> ',count) as cityname").where(c7("userid").notEqual(""))
    val e7 = d7.groupBy("provincename").count()
    val res7 = e7.selectExpr("provincename")
   // res7.show()
    val f7 = d7.groupBy("cityname").count()
    val res8 = f7.selectExpr("cityname")
   // res8.show()


    val vv = dv.groupBy("id","long","lat").count()
    val vv1 = vv.selectExpr("id","concat(cast(long as decimal(20,6)),',',cast(lat as decimal(20,6))) as aaa").where(vv("long").isNotNull and vv("lat").isNotNull and vv("long").notEqual(0) and vv("lat").notEqual(0))
    val vv2 = vv1.selectExpr("id","concat('https://restapi.amap.com/v3/geocode/regeo?location=',aaa,'&key=70eeb9c3349318273a467cbed0c1592a&radius=1000&extensions=all') as aaa").where(vv1("id").isNotNull)
    import sparkSession.implicits._
    val vv3 = vv2.map{
      x=>(HttpUtil.get(x.getAs[String]("aaa")))

    }

    val vv4 = vv2.select(vv2("id"),row_number().over(w3).as("he"))
    val vv5 = vv3.select(vv3("value"),row_number().over(w2).as("he"))


    val vv6 = vv4.join(vv5,vv4("he")===vv5("he"))
    val rr1 = vv6.select("value","id")


    val rr2 = rr1.select(rr1("id"),json_tuple(rr1("value"),"regeocode","status"))
    val rr3 = rr2.select(rr2("id"),get_json_object(rr2("c0"),"$.addressComponent").as("cn"))
    val rr4 = rr3.select(rr3("id"),get_json_object(rr3("cn"),"$.businessAreas").as("cc"))
    val rr5 = rr4.select(rr4("id"),rr4("cc")).where(rr4("cc").isNotNull and rr4("cc").notEqual("[[]]"))
//    val rrr1 = rr5.select(rr5("id"),explode(split(rr5("cc"),"\\[")).as("aa"))
//    val rrr2 = rrr1.select("id","aa").where(rrr1("aa").notEqual(""))
//    val rrr3 =rrr2.select(rrr2("id"),explode(split(rrr2("aa"),"\\]")).as("aa"))
//    val rrr4 = rrr3.select("id","aa").where(rrr3("aa").notEqual(""))
//    val rr6 = rrr4.select(rrr4("id"),get_json_object(rrr4("aa"),"$.name").as("cv"))
//    val rr7 = rr6.groupBy("id","cv").count()
//    val rr8 = rr7.select(rr7("id"),rr7("cv"),rr7("count").as("num"))
//    val rr9 = rr8.selectExpr("id","concat(cv,' -> ',num) as shangquan")
    val rr6 = rr5.select(rr5("id"),explode(split(rr5("cc"),",")).as("cc"))
    val rr7 = rr6.select(rr6("id"),rr6("cc")).where(rr6("cc").startsWith("\"name\""))
    val rr8 = rr7.select(rr7("id"),substring_index(rr7("cc"),":",-1).as("name"))
    val rr9 = rr8.select(rr8("id"),explode(split(rr8("cc"),"\"")).as("cc"))
//    val res9 = h6.join(b5, "userid").join(b4, "userid").join(b3, "userid").join(c2, "userid")
//      .join(d, "userid").join(c1, "userid").join(c7, "userid")
//    val res10 = res9.join(dv,"userid").select(res9("userid"),res9("keywords"),res9("ispname"),res9("networkmannername"),res9("client"),res9("adplatformproviderid"),res9("adspacetypename"),res9("adspacetype"),res9("appname"),res9("provincename"),res9("cityname"),dv("id"))
//    val res11 = res10.join(rr9,rr9("id")===res10("id"),"leftouter")
//    val res12 = res11.selectExpr("concat('userid :',userid,' ',keywords,':',ispname,':',networkmannername,':',client,':',adplatformproviderid,':',adspacetypename,':',adspacetype,':',appname,':',provincename,':',cityname,':',shangquan) as biaoqian")
//
//    rr9.show()
//    res12.show()


  }

}

