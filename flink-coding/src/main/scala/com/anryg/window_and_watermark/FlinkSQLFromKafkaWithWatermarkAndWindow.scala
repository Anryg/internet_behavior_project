package com.anryg.window_and_watermark

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}


/**
  * @DESC: 用SQL API读取kafka数据，并利用watermark和window功能来对数据进行统计
  * @Auther: Anryg
  * @Date: 2022/8/14 19:08
  */
object FlinkSQLFromKafkaWithWatermarkAndWindow {

    def main(args: Array[String]): Unit = {
        val streamingSetting = EnvironmentSettings.newInstance().inStreamingMode().build()

        val config = new Configuration() //设置checkpoint
        config.setString("execution.checkpointing.interval","10000")
        config.setString("state.backend", "filesystem")
        config.setString("state.checkpoints.dir","hdfs://192.168.211.106:8020/tmp/checkpoint/FlinkSQLFromKafkaWithWatermarkAndWindow")

        streamingSetting.getConfiguration.addAll(config)

        val tableEnv = TableEnvironment.create(streamingSetting)

        tableEnv.executeSql(
            """
              |Create table kafkaTable(
              |client_ip STRING,
              |domain STRING,
              |`time` STRING,
              |target_ip STRING,
              |rcode STRING,
              |query_type STRING,
              |authority_record STRING,
              |add_msg STRING,
              |dns_ip STRING,
              |event_time AS to_timestamp(`time`, 'yyyyMMddHHmmss'), //设置事件时间为实际数据的产生时间，注意time这个字段必须要用``符合括起来
              |watermark for event_time as event_time - interval '10' second  //设置watermark，确定watermark字段
              |)
              |with(
              |'connector' = 'kafka',
              |'topic' = 'qianxin',
              |'properties.bootstrap.servers' = '192.168.211.107:6667',
              |'properties.group.id' = 'FlinkSQLFromKafkaWithWatermarkAndWindow',
              |'scan.startup.mode' = 'latest-offset',
              |'value.format'='csv',                                 //确定数据源为文本格式
              |'value.csv.field-delimiter'='|'                      //确定文本数据源的分隔符
              |)
            """.stripMargin)

        tableEnv.executeSql(
            """
              |SELECT
              |window_start,
              |window_end,
              |client_ip,
              |count(client_ip) as ip_count
              |FROM TABLE(
              |HOP(                       //确定window策略
              |TABLE kafkaTable,
              |DESCRIPTOR(event_time),
              |INTERVAL '30' SECONDS,   //确定滑动周期
              |INTERVAL '2' MINUTES)    //确定窗口时间间隔
              |)
              |GROUP BY
              |window_start,
              |window_end,
              |client_ip
              |ORDER BY ip_count
              |DESC
              |LIMIT 10
            """.stripMargin
        ).print()

    }
}
