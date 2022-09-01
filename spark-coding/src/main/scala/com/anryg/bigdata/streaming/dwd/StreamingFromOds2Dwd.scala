package com.anryg.bigdata.streaming.dwd

import com.anryg.bigdata.{IpSearch, RedisClientUtils}
import com.anryg.bigdata.streaming.StreamingProcessHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * @DESC: 读取ods层数据，并加工写入到dwd
  * @Auther: Anryg
  * @Date: 2022/9/1 09:53
  */
object StreamingFromOds2Dwd  extends StreamingProcessHelper[Any]{

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("StreamingFromOds2Dwd").setMaster("local")
        val spark = SparkSession.builder().config(conf)
                .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://hdp01.pcl-test.com:2181,hdp03.pcl-test.com:2181,hdp02.pcl-test.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")
                .config("spark.datasource.hive.warehouse.metastoreUri","thrift://hdp01.pcl-test.com:9083")
                .enableHiveSupport() //打开hive支持功能，可以与hive共享catalog
                .getOrCreate()

        clickProcess(spark,"ods.ods_kafka_internetlog","dwd.dwd_internetlog_detail")
    }

    /**
      *@DESC: 流方式读取hive数据源
      * */
    def readHive2DF(sparkSession: SparkSession, sourceTable:String): DataFrame ={
        sparkSession.readStream.table(sourceTable)
    }

    /**
      *@DESC: 对ods数据进行字段补齐等处理
      * */
    def handleData(sparkSession: SparkSession, dataFrame: DataFrame, tableName:String): DataFrame ={
        import sparkSession.implicits._
        dataFrame.printSchema()
        dataFrame.map(row => {
            val clientIP = row.getAs[String]("client_ip")
            val ipAndAddr = IpSearch.getAddrByIP(RedisClientUtils.getSingleRedisClient,clientIP).split("-")
            val country = ipAndAddr(2)
            val province = ipAndAddr(3)
            val city = ipAndAddr(4)
            val operator = ipAndAddr(5)
            val domain = row.getAs[String]("domain").toLowerCase//将域名转成小写
            val time = row.getAs[String]("time")
            val targetIP = row.getAs[String]("target_ip")
            val rcode = row.getAs[String]("rcode")
            val queryType = row.getAs[String]("query_type")
            val authRecord = row.getAs[String]("authority_record").toLowerCase
            val addMsg = row.getAs[String]("add_msg")
            val dnsIP = row.getAs[String]("dns_ip")
            val year = row.getAs[String]("year")
            val month = row.getAs[String]("month")
            val day = row.getAs[String]("day")
            (clientIP,country,province,city,operator,domain,time,targetIP,rcode,queryType,authRecord,addMsg,dnsIP,year,month,day)
        }).toDF("client_ip","country","province","city","operator","domain","time","target_ip","rcode","query_type","authority_record","add_msg","dns_ip","year","month","day")
    }

    /**
      *@DESC: 将处理好的数据sink到dwd表中
      * */
    def sinkData(targetDS:DataFrame, tableName:String): StreamingQuery ={
        val config = Map(("checkpointLocation","hdfs://192.168.211.106:8020/tmp/offset/test/StreamingFromOds2Dwd"),
            ("format","append"))
        getStreamingWriter(targetDS,OutputMode.Append(),"orc",config)
                .partitionBy("year","month","day")
                .toTable(tableName)
    }

    /**
      * @DESC: 将所有数据步骤串起来
      * */
    def clickProcess(sparkSession: SparkSession,sourceTable:String, sinkTable:String): Unit ={
        val rawDF = readHive2DF(sparkSession, sourceTable)
        val targetDS = handleData(sparkSession, rawDF, sourceTable)
        sinkData(targetDS, sinkTable).awaitTermination()
    }

}
