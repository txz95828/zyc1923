package com.CORE.Tag

import ch.hsr.geohash.GeoHash
import com.CORE.util.{AmapUtil, String2Type, Tag}
import com.CORE.util.{AmapUtil, JedisConnectionPool, String2Type, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object BusinessTag extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    if(String2Type.toDouble(row.getAs[String]("long")) >=73
      && String2Type.toDouble(row.getAs[String]("long")) <=136
      && String2Type.toDouble(row.getAs[String]("lat"))>=3
      && String2Type.toDouble(row.getAs[String]("lat"))<=53){
      val long = row.getAs[String]("long").toDouble
      val lat = row.getAs[String]("lat").toDouble
      val business = getBusiness(long,lat)
      if(StringUtils.isNoneBlank(business)){
        val str = business.split(",")
        str.foreach(str=>{
          list:+=(str,1)
        })
      }
    }
    list
  }
  def getBusiness(long:Double,lat:Double):String={
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)
    var business = redis_queryBusiness(geohash)
    if(business == null){
      business = AmapUtil.getBusinessFromAmap(long,lat)
      if(business!=null && business.length>0){
        redis_insertBusiness(geohash,business)
      }
    }
    business
  }
  def redis_queryBusiness(geohash:String):String={
    val jedis = JedisConnectionPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }
  def redis_insertBusiness(geohash:String,business:String): Unit ={
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geohash,business)
    jedis.close()
  }
}
