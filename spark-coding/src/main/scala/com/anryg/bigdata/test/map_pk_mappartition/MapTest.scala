package com.anryg.bigdata.test.map_pk_mappartition

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @DESC:
  * @Auther: Anryg
  * @Date: 2022/9/20 10:10
  */
object MapTest {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MapTest")/*.setMaster("local")*/
        val spark = SparkSession.builder().config(conf).getOrCreate()
        val rawDF = spark.read/*.option("header",true)*/.csv(args(0))//读取HDFS的数据源

        import spark.implicits._
        rawDF.printSchema() //spark job 1
        rawDF.show() //spark job 2
        val resultDF = rawDF.map(row => {
            val clientIP = row.getAs[String]("_c0")
            val domain = row.getAs[String]("_c1").toLowerCase//将域名转成小写
            val time = row.getAs[String]("_c2")
            val targetIP = row.getAs[String]("_c3")
            val rcode = row.getAs[String]("_c4")
            val queryType = row.getAs[String]("_c5")
            val authRecord = if (row.getAs[String]("_c6") == null ) "" else row.getAs[String]("_c6").toLowerCase
            val addMsg = if (row.getAs[String]("_c7") == null ) "" else row.getAs[String]("_c7")
            val dnsIP = row.getAs[String]("_c8")
            (clientIP,domain,time,targetIP,rcode,queryType,authRecord,addMsg,dnsIP)
        }).toDF("client_ip","domain","time","target_ip","rcode","query_type","authority_record","add_msg","dns_ip")


        /**将转换后的数据写入HDFS*/
        resultDF.write.csv(args(1))//spark job 3
    }
}
