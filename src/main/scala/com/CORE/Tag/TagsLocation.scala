package com.CORE.Tag

import com.CORE.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsLocation extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val pro = row.getAs[String]("provincename")
    val city = row.getAs[String]("cityname")
    if(StringUtils.isNotBlank(pro)){
      list :+=("ZP"+pro,1)
    }
    if(StringUtils.isNotBlank(city)){
      list :+=("ZC"+city,1)
    }
    list
  }
}
