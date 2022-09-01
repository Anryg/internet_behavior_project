package com.anryg.bigdata.streaming.ods

import com.alibaba.fastjson.JSON
import com.anryg.bigdata.streaming.StreamingProcessHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * @DESC:
  * @Auther: Anryg
  * @Date: 2022/8/31 19:03
  */
object StreamingSource2HiveOds extends StreamingProcessHelper[Any]{


    /**
      * @DESC: 主函数，应用运行入口
      * */
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("StreamingSource2HiveOds").setMaster("local[*]")
        val spark = SparkSession.builder().config(conf)
                .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://hdp01.pcl-test.com:2181,hdp03.pcl-test.com:2181,hdp02.pcl-test.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")
                .config("spark.datasource.hive.warehouse.metastoreUri","thrift://hdp01.pcl-test.com:9083")
                .enableHiveSupport() //打开hive支持功能，可以与hive共享catalog
                .getOrCreate()

        clickProcess(spark)

    }

    /**
      *@DESC: 将kafka数据源数据读取出来成为DataFrame
      * */
    def readKafka2DF(sparkSession: SparkSession): DataFrame ={
        val config = Map(("kafka.bootstrap.servers", "192.168.211.107:6667"),("subscribe","test"),
            ("failOnDataLoss","false"),("fetchOffset.numRetries","3"),("startingOffsets","earliest"))

        getStreamingReader(sparkSession,"kafka",config).load()
    }

    /**
      *@DESC: 将原始DF进行业务逻辑处理
      * */

    def handleData(sparkSession: SparkSession, rawDF:DataFrame): DataFrame ={
        import sparkSession.implicits._
        val targetDS = rawDF.selectExpr("CAST(value AS STRING)")  //将kafka中的数据的value转为为string，原始为binary类型
                .map(row => {
            val line = row.getAs[String]("value") //获取row对象中的field，其实也只有一个field
            val rawJson = JSON.parseObject(line)      //原始string是一个json，对其进行解析
            val message = rawJson.getString("message")  //获取业务数据部分
            val msgArray = message.split(",")  //指定分隔符进行字段切分
            msgArray
        }).filter(_.length == 9).filter(array => array(2).length >= 8)//确保日期字段符合规范
                .map(array =>(array(0),array(1),array(2),array(3), array(4),array(5),array(6),array(7),array(8),
                array(2).substring(0,4),array(2).substring(4,6),array(2).substring(6,8)))
                .toDF("client_ip","domain","time","target_ip","rcode","query_type","authority_record","add_msg","dns_ip","year","month","day") //给裸数据添加字段名

        targetDS
    }

    /**
      *@DESC: 将目标数据集进行写入hive的ODS
      * */
    def sinkData(targetDS:DataFrame): StreamingQuery ={
        val config = Map(("checkpointLocation","hdfs://192.168.211.106:8020/tmp/offset/test/StreamingSource2HiveOds"),
            ("format","append"))
        getStreamingWriter(targetDS, OutputMode.Append(),"orc",config)
                .partitionBy("year","month","day")
                .toTable("ods.ods_kafka_internetlog")
    }

    /**
      * @DESC: 将所有数据步骤串起来
      * */
    def clickProcess(sparkSession: SparkSession): Unit ={
        val rawDF = readKafka2DF(sparkSession)
        val targetDS = handleData(sparkSession, rawDF)
        sinkData(targetDS).awaitTermination()
    }

}
