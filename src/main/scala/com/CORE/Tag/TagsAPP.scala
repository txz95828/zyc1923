package com.CORE.Tag

import com.CORE.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsAPP extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val appdoc = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else {
      list:+=("APP"+appdoc.value.getOrElse(appid,"其他"),1)
    }
    list
  }
}