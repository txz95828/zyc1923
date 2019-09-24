package com.CORE.Tag

import com.CORE.util.TagUtils
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
object TagsContext2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Tags").master("local").getOrCreate()
    import spark.implicits._
//    val load = ConfigFactory.load()
//    val HbaseTableName = load.getString("HBASE.tableName")
//    val configuration = spark.sparkContext.hadoopConfiguration
//    configuration.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))
//    val hbConn = ConnectionFactory.createConnection(configuration)
//    val hbadmin = hbConn.getAdmin
//    if(!hbadmin.tableExists(TableName.valueOf(HbaseTableName))){
//      println("当前表可用")
//      val tableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))
//      val hColumnDescriptor = new HColumnDescriptor("tags")
//      tableDescriptor.addFamily(hColumnDescriptor)
//      hbadmin.createTable(tableDescriptor)
//      hbadmin.close()
//      hbConn.close()
//    }
//    val conf = new JobConf(configuration)
//    conf.setOutputFormat(classOf[TableOutputFormat])
//    conf.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)



    val df = spark.read.parquet("D:\\SparkProject\\test6")
    val docsRDD = spark.sparkContext.textFile("C:\\Users\\86188\\Documents\\Tencent Files\\1474452338\\FileRecv\\textLog.log").map(_.split("\\s")).filter(_.length>=5)
      .map(arr=>(arr(4),arr(1))).collectAsMap()
    val broadValue = spark.sparkContext.broadcast(docsRDD)
    val stopwordsRDD = spark.sparkContext.textFile("C:\\Users\\86188\\Documents\\Tencent Files\\1474452338\\FileRecv\\textLog.log").map((_,0)).collectAsMap()
    val broadValues = spark.sparkContext.broadcast(stopwordsRDD)
    val allUserId = df.rdd.map(row=>{
      val strList = TagUtils.getallUserId(row)
      (strList,row)
    })
    val verties = allUserId.flatMap(row=>{
      val rows = row._2
      val adList = TagsAd.makeTags(rows)
      val businessList = BusinessTag.makeTags(rows)
      val appList = TagsAPP.makeTags(rows,broadValue)
      val devList = TagsDevice.makeTags(rows)
      val locList = TagsLocation.makeTags(rows)
      val kwList = TagsKword.makeTags(rows,broadValues)
      val tagList = adList++ appList++devList++locList++kwList
      val VD = row._1.map((_,0))++tagList

      row._1.map(uId=>{
        if(row._1.head.equals(uId)){
          (uId.hashCode.toLong,VD)
        }else{
          (uId.hashCode.toLong,List.empty)
        }
      })
    })
    verties.take(220).foreach(println)
    val edges = allUserId.flatMap(row=>{
      row._1.map(uId=>Edge(row._1.head.hashCode.toLong,uId.hashCode.toLong,0))
    })
    edges.take(20).foreach(println)
    val graph = Graph(verties,edges)
    val vertices = graph.connectedComponents().vertices
    vertices.join(verties).map{
      case (uid,(cnId,tagsAndUserId))=>{
        (cnId,tagsAndUserId)
      }
    }.reduceByKey(
      (list1,list2)=>{
        (list1++list2)
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toList
      }).map{
      case (userId,userTags) =>{
        val put = new Put(Bytes.toBytes(userId))
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes("2016-10-01 06:19:17"),Bytes.toBytes(userTags.mkString(",")))
        (new ImmutableBytesWritable(),put)
      }
    }
//      .saveAsHadoopDataset(conf)

    spark.stop()
  }
}