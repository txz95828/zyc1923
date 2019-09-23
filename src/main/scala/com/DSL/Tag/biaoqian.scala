package com.DSL.Tag


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._


object biaoqian {
  def main(args: Array[String]): Unit = {
    val Array(inputPath) = args

    val sparkSession = SparkSession.builder().master("local[*]").appName("one").getOrCreate()
    val df = sparkSession.read.parquet(inputPath)
    val w1 =Window.orderBy("userid")
    val w2 = Window.orderBy("value")
    val w3 = Window.orderBy("id")
    val dv = df.withColumn("id",row_number().over(w1))

    //广告
    val a = dv.groupBy("id","userid", "adspacetype", "adspacetypename").count()
    val b = a.select("id","userid", "adspacetype", "adspacetypename", "count")
    val c = b.select(b("id"),b("userid"), lpad(b("adspacetype"), 2, "0").as("adspacetype"), b("adspacetypename"), b("count").as("num"))
    val d = c.selectExpr("id","userid", "concat('LN',adspacetypename) as adspacetypename", "concat('LC',adspacetype) as adspacetype","num")
    val e = d.selectExpr("id","userid","concat('(',adspacetype,',',num,')') as adspacetype", "concat('(',adspacetypename,',',num,')') as adspacetypename")
    val f = e.groupBy("adspacetype", "adspacetypename").count()
    val res1 = f.selectExpr("adspacetype", "adspacetypename")
   // res1.show()
    //APP
    val a1 = dv.groupBy("id","userid", "appname").count()
    val b1 = a1.select(a1("id"),a1("userid"), a1("appname"), a1("count").as("num"))
    val c1 = b1.selectExpr("id","userid", "concat('APP',appname) as appname","num")
    val d1 = b1.selectExpr("id","userid","concat('(APP',appname,',',num,')') as appname")
    val e1 = d1.groupBy("appname").count()
    val res2 = e1.selectExpr("appname")
   // res2.show()

    //渠道
    val a2 = dv.groupBy("id","userid", "adplatformproviderid").count()
    val b2 = a2.select(a2("id"),a2("userid"), a2("adplatformproviderid"),a2("count").as("num"))
    val c2 = b2.selectExpr("id","userid", "concat('CN',adplatformproviderid) as adplatformproviderid","num")
    val d2 = b2.selectExpr("id","userid","concat('(CN',adplatformproviderid,',',num,')') as adplatformproviderid")
    val e2 = d2.groupBy("adplatformproviderid").count()
    val res3 = e2.selectExpr("adplatformproviderid")
    //res3.show()




    //客户端
    val a3 = dv.groupBy("id","userid", "client").count()
    val b3 = a3.selectExpr("id","userid", "(case when client = 1 then 'D00010001' when client = 2 then 'D00010002' when client = 3 then 'D00010003' when client = 4 then 'D00010004' end) as client","count as num")
    val c3 = b3.selectExpr("id","userid","concat('(',client,',',num,')') as client")
    val d3 = c3.groupBy("client").count()
    val res4 = d3.selectExpr("client")
   // res4.show()

    //流量型号
    val a4 = dv.groupBy("id","userid", "networkmannername").count()
    val b4 = a4.selectExpr("id","userid", "(case when networkmannername = '2G' then 'D00020004' when networkmannername = '3G' then 'D00020003' when networkmannername = '4G' then 'D00020002' when networkmannername = 'Wifi' then 'D00020001' else  'D00020005' end) as networkmannername","count as num")
    val c4 = b4.selectExpr("id","userid","concat('(',networkmannername,',',num,')') as networkmannername")
    val d4 = c4.groupBy("networkmannername").count()
    val res5 = d4.selectExpr("networkmannername")
    //res5.show()

    //网络供应商
    val a5 = dv.groupBy("id","userid", "ispname").count()
    val b5 = a5.selectExpr("id","userid", "(case when ispname = '移动' then 'D00030001'when ispname = '联通' then 'D00030002 ' when ispname = '电信' then 'D00030003' else 'D00030004' end) as ispname","count as num")
    val c5 = b5.selectExpr("id","userid","concat('(',ispname,',',num,')') as ispname")
    val d5 = c5.groupBy("ispname").count()
    val res6 = d5.selectExpr("ispname")
    //res6.show()

    //关键字
    val a6 = dv.groupBy("id","userid", "keywords").count()
    val b6 = a6.select(a6("id"),a6("userid"), explode(split(a6("keywords"), "\\|")).as("keywords"))
    val c6 = b6.groupBy("id","userid", "keywords").count()
    val re = c6.groupBy("id","userid").count()
    val re1 = re.select(re("id"),re("userid"), re("count").as("num"))
    val re2 = re1.join(c6, "id")
    val re3 = re2.select(c6("id"),c6("userid"),re2("keywords"),re2("count"))
    val d6 = re3.selectExpr("id","userid", "concat(keywords,':',count) as keywords")
    val e6 = d6.groupBy("id","userid").agg(collect_set(d6("keywords")))
    val f6 = e6.select(e6("id"),e6("userid"), e6("collect_set(keywords)").as("keywords"))
    val g6 = f6.select(f6("id"),f6("userid"), concat_ws(",", f6("keywords")).as("keywords"))
    val h6 = g6.selectExpr("id","userid", "concat('(K ',keywords,')') as keywords")
    val i6 = h6.selectExpr("id","userid","(case when keywords='(K :1)' then ' ' else keywords end) as keywords")

    //省份，城市
    val a7 = dv.groupBy("id","userid", "provincename", "cityname").count()
    val b7 = a7.select("id","userid", "provincename", "cityname","count")
    val c7 = b7.selectExpr("id","userid", "concat('ZP',provincename) as provincename", "concat('ZC',cityname) as cityname","count")
    val d7 = c7.selectExpr("id","userid","concat('(',provincename,',',count,')') as provincename", "concat('(',cityname,',',count,')') as cityname")
    val e7 = d7.groupBy("provincename").count()
    val res7 = e7.selectExpr("provincename")
   // res7.show()
    val f7 = d7.groupBy("cityname").count()
    val res8 = f7.selectExpr("cityname")
   // res8.show()

    //商圈
    val vv = dv.groupBy("id","long","lat").count()
    val vv1 = vv.selectExpr("id","concat(cast(long as decimal(20,6)),',',cast(lat as decimal(20,6))) as aaa").where(vv("long").isNotNull and vv("lat").isNotNull and vv("long").notEqual(0) and vv("lat").notEqual(0))
    val vv2 = vv1.selectExpr("id","concat('https://restapi.amap.com/v3/geocode/regeo?location=',aaa,'&key=70eeb9c3349318273a467cbed0c1592a&radius=1000&extensions=all') as aaa").where(vv1("id").isNotNull)
    import sparkSession.implicits._
    val vv3 = vv2.map{
      x=>(HttpUtil.get(x.getAs[String]("com")))

    }
    val vv4 = vv2.select(vv2("id"),row_number().over(w3).as("he"))
    val vv5 = vv3.select(vv3("value"),row_number().over(w2).as("he"))
    val vv6 = vv4.join(vv5,vv4("he")===vv5("he"))
    val rr1 = vv6.select("value","id")
    val rr2 = rr1.select(rr1("id"),json_tuple(rr1("value"),"regeocode","status"))
    val rr3 = rr2.select(rr2("id"),get_json_object(rr2("c0"),"$.addressComponent").as("cn"))
    val rr4 = rr3.select(rr3("id"),get_json_object(rr3("cn"),"$.businessAreas").as("cc"))
    val rr5 = rr4.select(rr4("id"),rr4("cc")).where(rr4("cc").isNotNull and rr4("cc").notEqual("[[]]"))
    val rr6 = rr5.select(rr5("id"),explode(split(rr5("cc"),",")).as("cc"))
    val rr7 = rr6.select(rr6("id"),rr6("cc")).where(rr6("cc").startsWith("\"name\""))
    val rr8 = rr7.select(rr7("id"),substring_index(rr7("cc"),":",-1).as("name"))
    val rr9 = rr8.select(rr8("id"),explode(split(rr8("name"),"\"")).as("name"))
    val rr10 = rr9.select(rr9("id"),rr9("name")).where(rr9("name").notEqual(""))
    val rr11 = rr10.groupBy(rr10("id")).agg(collect_set(rr10("name")))
    val rr12 = rr11.select(rr11("id"),rr11("collect_set(name)").as("name"))
    val rr13 = rr12.select(rr12("id"),concat_ws(",",rr12("name")).as("name"))
    val rr14 = rr13.groupBy("id","name").count()
    val rr15 = rr14.select(rr14("id"),rr14("name"),rr14("count").as("num"))
    val rr16 = rr15.selectExpr("id","concat(name,' -> ',num) as shangquan")


    //Utils
    val a8 = dv.groupBy("id","userid","imei","mac","idfa","openudid","androidid","imeimd5","macmd5","idfamd5","openudidmd5",
      "androididmd5","imeisha1","macsha1","idfasha1","openudidsha1","androididsha1").count()
   val b8 = a8.selectExpr("id","userid","(case when imei = '' then '' else concat('(IM',imei,',0)') end ) as imei",
    "(case when mac = '' then '' else concat('(MC',mac,',0)') end) as mac",
    "(case when idfa = '' then '' else concat('(ID',idfa,',0)') end) as idfa",
    "(case when openudid = '' then '' else concat('(OD',openudid,',0)') end) as openudid",
    "(case when androidid = '' then '' else concat('(AD',androidid,',0)') end) as androidid",
    "(case when imeimd5 = '' then '' else concat('(IM',imeimd5,',0)') end) as imeimd5",
    "(case when macmd5 = '' then '' else concat('(MC',macmd5,',0)') end) as macmd5",
    "(case when idfamd5 = '' then '' else concat('(ID',idfamd5,',0)') end) as idfamd5",
    "(case when openudidmd5 = '' then '' else concat('(OD',openudidmd5,',0)') end) as openudidmd5",
    "(case when androididmd5 = '' then '' else concat('(AD',androididmd5,',0)') end) as androididmd5",
    "(case when imeisha1 = '' then '' else concat('(IM',imeisha1,',0)') end) as imeisha1",
    "(case when macsha1 = '' then '' else concat('(MC',macsha1,',0)') end) as macsha1",
    "(case when idfasha1 = '' then '' else concat('(ID',idfasha1,',0)') end) as idfasha1",
    "(case when openudidsha1 = '' then '' else concat('(OD',openudidsha1,',0)') end) as openudidsha1",
    "(case when androididsha1 = '' then '' else concat('(AD',androididsha1,',0)') end) as androididsha1")


    //整合
    val res9 = i6.join(c5, "id").join(c4,"id").join(c3,"id").join(d2,"id")
      .join(e, "id").join(d1, "id").join(d7, "id").join(b8,"id")
    val res10 = res9.select(dv("id"),hash(dv("userid")).as("userid"),hash(res9("imei")).as("cn"),concat_ws(",",res9("imei"),res9("mac"),res9("idfa"),res9("openudid"),res9("androidid"),res9("imeimd5"),res9("macmd5"),res9("idfamd5"),res9("openudidmd5"),
     res9("androididmd5"),res9("imeisha1"),res9("macsha1"),res9("idfasha1"),res9("openudidsha1"),res9("androididsha1"),res9("keywords"),res9("ispname"),res9("networkmannername"),res9("client"),res9("adplatformproviderid"),res9("adspacetype"),res9("adspacetypename"),res9("appname"),res9("provincename"),res9("cityname")).as("biaoqian"))
    val res11 = res10.select(res10("cn"),res10("userid"),res10("biaoqian")).where(res10("userid").contains("142593372") or res10("userid").contains("-2041702761"))
   val res12 = res10.select(res10("cn"),res10("userid"),res10("biaoqian")).where(res10("userid").notEqual("142593372") or res10("userid").notEqual("-2041702761"))
    val res13 = res11.select(res11("cn"),res11("userid"),format_string(" ",res11("biaoqian")).as("biaoqian"))
   val res14 = res13.union(res12)
   val res15 = res14.select(res14("cn").as("ID"),res14("biaoqian")).orderBy(rand()*10000)
   res15.show()

  }

}

