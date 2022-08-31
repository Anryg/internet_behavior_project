package com.anryg.bigdata.streaming

import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
  * @DESC:  从kafka读取上网数据,写入hive动态分区表
  * @Auther: Anryg
  * @Date: 2020/12/17 09:56
  */
object StructuredStreamingReadHive {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName("StructuredStreamingReadHive")
                .setMaster("local[*]")//本地运行模式，如果提交集群，注释掉这行
        val spark = SparkSession.builder().config(conf)
                .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://hdp01.pcl-test.com:2181,hdp03.pcl-test.com:2181,hdp02.pcl-test.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")
                .config("spark.datasource.hive.warehouse.metastoreUri","thrift://hdp01.pcl-test.com:9083")
                .enableHiveSupport() //打开hive支持功能，可以与hive共享catalog
                .getOrCreate()

         val rawDF = spark.readStream
                 .table("test.test")
                 .select("client_ip")
                 .writeStream
                 .format("console")
                 .option("checkpointLocation","hdfs://192.168.211.106:8020/tmp/offset/test/StructuredStreamingReadHive")
                 .start().awaitTermination()

    }

}
