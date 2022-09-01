package com.anryg.bigdata.streaming.demo;
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
/**
  * @DESC:  对接实时上网的数据源到HDFS的CSV文件中
  * @Auther: Anryg
  * @Date: 2020/12/17 09:56
  */
object StructuredStreaming4Kafka2CSV {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("StructuredStreaming4Kafka2CSV").setMaster("local[*]")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        val rawDF = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "192.168.211.107:6667")
            .option("subscribe","qianxin")
            //.option("group.id","test9999") /**不再用该方式来绑定offset，而是每个程序有个唯一的id，该id跟checkpointLocation绑定，虽然group.id属性在运行中依然保留，但是不再跟offset绑定*/
            .option("failOnDataLoss",false)
            .option("fetchOffset.numRetries",3)
            .option("maxOffsetsPerTrigger",90000000)/**用于限流，限定每个批次取的数据条数，确定写入HDFS单个文件的条数*/
            .option("startingOffsets","earliest")
            .load()

        import spark.implicits._
        val ds = rawDF.selectExpr("CAST(value AS STRING)")
                        .map(row => {
                            val line = row.getAs[String]("value")
                            val fieldArray:Array[String] = line.split("\\|")
                            fieldArray
                        }).filter(_.length == 9).map(array =>(array(0),array(1),array(2),array(3),array(4),array(5),array(6),array(7),array(8)))
                .toDF("client_ip","domain","time","target_ip","rcode","query_type","authority_record","add_msg","dns_ip")

        ds.printSchema()

        //val ds1 = ds.select($"client_ip")
        val query = ds.writeStream
            .outputMode(OutputMode.Append()).trigger(Trigger.ProcessingTime(60,TimeUnit.SECONDS))/**每60秒写文件一次*/
                .option("format", "append") /**会在同一个目录下追加新文件，否则只能在特定目录下写一个批次的的数据后就报错*/
                .option("header", "true") /**添加文件的scheme*/
            .format("csv").option("path","hdfs://192.168.211.106:8020/DATA/qianxin/3/")
            .option("checkpointLocation","hdfs://192.168.211.106:8020/tmp/offset/test/kafka_datasource-03") /**用来保存offset，用该目录来绑定对应的offset，如果该目录发生改变则程序运行的id会发生变化，类比group.id的变化*/
            .start()

        query.awaitTermination()
    }

}
