package com.anryg.bigdata.test.map_pk_mappartition

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * @DESC:
  * @Auther: Anryg
  * @Date: 2022/9/20 10:10
  */
object MapPartitionTest {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MapPartitionTest")/*.setMaster("local")*/
        val spark = SparkSession.builder().config(conf).getOrCreate()
        val rawDF = spark.read/*.option("header",true)*/.csv(args(0))

        import spark.implicits._
        rawDF.printSchema()
        rawDF.show()
        val resultDF = rawDF.mapPartitions(iterator => {
            //val array = new mutable.ArrayBuffer[(String,String,String,String,String,String,String,String,String)]
            //val seq = mutable.Seq[(String,String,String,String,String,String,String,String,String)]
            //val list = new util.LinkedList[(String,String,String,String,String,String,String,String,String)]
            val set = new mutable.LinkedHashSet[(String,String,String,String,String,String,String,String,String)]
            while (iterator.hasNext){
                val next = iterator.next()
                val clientIP = next.getAs[String]("_c0")
                val domain = next.getAs[String]("_c1").toLowerCase//将域名转成小写
                val time = next.getAs[String]("_c2")
                val targetIP = next.getAs[String]("_c3")
                val rcode = next.getAs[String]("_c4")
                val queryType = next.getAs[String]("_c5")
                val authRecord = if (next.getAs[String]("_c6") == null ) "" else next.getAs[String]("_c6").toLowerCase
                val addMsg = if (next.getAs[String]("_c7") == null ) "" else next.getAs[String]("_c7")
                val dnsIP = next.getAs[String]("_c8")

                set.+=((clientIP,domain,time,targetIP,rcode,queryType,authRecord,addMsg,dnsIP))
                //array.+=((clientIP,domain,time,targetIP,rcode,queryType,authRecord,addMsg,dnsIP))
            }
            //array.toIterator
            set.toIterator
        }).toDF("client_ip","domain","time","target_ip","rcode","query_type","authority_record","add_msg","dns_ip")

        resultDF.write.csv(args(1))
    }



}
