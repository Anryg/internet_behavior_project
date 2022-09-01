package com.anryg.bigdata.streaming.demo

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
;

/**
  * @DESC:  从kafka读取上网上网数据
  * @Auther: Anryg
  * @Date: 2020/12/17 09:56
  */
object StructuredStreamingFromKafka {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("StructuredStreamingFromKafka").setMaster("local[*]")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        val rawDF = spark.readStream //获取数据源
            .format("kafka") //确定数据源的来源格式
            .option("kafka.bootstrap.servers", "192.168.211.108:6667") //指定kafka集群的地址，理论上写一个broker就可以了
            .option("subscribe","test")  //指定topic
            //.option("group.id","test9999") /**不再用该方式来绑定offset，而是每个程序有个唯一的id，该id跟checkpointLocation绑定，虽然group.id属性在运行中依然保留，但是不再跟offset绑定*/
            .option("failOnDataLoss",false)  //如果读取数据源时，发现数据突然缺失，比如被删，则是否马上抛出异常
            .option("fetchOffset.numRetries",3)  //获取消息的偏移量时，最多进行的重试次数
            .option("maxOffsetsPerTrigger",10)/**用于限流，限定每次读取数据的最大条数，不指定则是as fast as possible,但是每次只取最新的数据，不取旧的*/
            .option("startingOffsets","latest")  //第一次消费时，读取kafka数据的位置
                //.option("startingOffsets","""{"test":{"0":-2,"1":-2,"2":-2,"3":-2}}""")
            .load()

        import spark.implicits._
        val ds = rawDF.selectExpr("CAST(value AS STRING)")  //将kafka中的数据的value转为为string，原始为binary类型
                        .map(row => {
                            val line = row.getAs[String]("value") //获取row对象中的field，其实也只有一个field
                            val rawJson = JSON.parseObject(line)      //原始string是一个json，对其进行解析
                            val message = rawJson.getString("message")  //获取业务数据部分
                            val msgArray = message.split(",")  //指定分隔符进行字段切分
                            msgArray
                        }).filter(_.length == 9)  //只留字段数为9的数据
                        .map(array => (array(0),array(1),array(2),array(3),array(4),array(5),array(6),array(7),array(8))) //将其转化成为元组，为了方便下一步赋予schema
                        .toDF("client_ip","domain","time","target_ip","rcode","query_type","authority_record","add_msg","dns_ip") //给裸数据添加字段名

        ds.printSchema() //打印schema，确认没有问题

        val query = ds.writeStream
            .outputMode(OutputMode.Append())  //指定数据的写入方式
                .format("console")  //指定外部输出介质
                //.trigger(Trigger.ProcessingTime(60,TimeUnit.SECONDS))/**每60秒执行一次，不指定就是as fast as possible*/
                .option("format", "append") /**会在同一个目录下追加新文件，否则只能在特定目录下写一个批次的的数据后就报错*/
                //.option("header", "true") /**添加文件的scheme*/
                // .format("csv").option("path","hdfs://192.168.211.106:8020/DATA/qianxin/3/")
            .option("checkpointLocation","hdfs://192.168.211.106:8020/tmp/offset/test/kafka_datasource-08") /**用来保存offset，用该目录来绑定对应的offset，如果该目录发生改变则程序运行的id会发生变化，类比group.id的变化*/
            .start()

        query.awaitTermination()
    }

}
