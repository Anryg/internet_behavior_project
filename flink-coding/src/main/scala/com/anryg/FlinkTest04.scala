package com.anryg

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


/**
  * @DESC: 读取kafka数据，从DataStream转为Table 把结果写ES
  * @Auther: Anryg
  * @Date: 2022/8/14 19:08
  */
object FlinkTest04 {
    case class InternetBehavior(id:String, client_ip:String, domain:String, do_time:String, target_ip:String,rcode:String, query_type:String, authority_record:String, add_msg:String, dns_ip:String)//定义当前数据对象

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val tableEnv = StreamTableEnvironment.create(env)

        val kafkaSource = KafkaSource.builder()
                        .setBootstrapServers("192.168.211.107:6667")
                        .setTopics("test")
                        .setGroupId("group01")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build()

        val kafkaDS = env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"kafka-data")
        val targetDS = kafkaDS.map(line => {
            val rawJson = JSON.parseObject(line)      //原始string是一个json，对其进行解析
            val message = rawJson.getString("message")  //获取业务数据部分
            val msgArray = message.split(",")  //指定分隔符进行字段切分
            msgArray
        }).filter(_.length == 9).map(array => {
            InternetBehavior(array(0)+array(1)+array(2),array(0),array(1),array(2),array(3),array(4),array(5),array(6),array(7),array(8))
        })

        val targetTable = tableEnv.fromDataStream(targetDS)//转化成为Table类型
        //targetTable.execute().print()

        /**定义sink*/
        tableEnv.executeSql("CREATE TABLE InternetBehavior (\n\tid String,\n  client_ip STRING,\n  domain STRING,\n  do_time STRING,\n  target_ip STRING,\n  rcode int,\n  query_type string,\n  authority_record string,\n  add_msg string,\n  dns_ip string,\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'elasticsearch-7',\n  'hosts' = 'http://192.168.211.106:9201',\n  'index' = 'internet_behavior-flink'\n)")

        targetTable.executeInsert("InternetBehavior")
        //targetDS.addSink()
        //targetTable.executeInsert()

        //env.execute("FlinkTest03")

    }
}
