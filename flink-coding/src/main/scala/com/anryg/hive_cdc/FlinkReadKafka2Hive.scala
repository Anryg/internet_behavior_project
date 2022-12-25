package com.anryg.hive_cdc

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
  * @DESC: Flink读取kafka写hive动态分区表
  * @Auther: Anryg
  * @Date: 2022/12/19 10:36
  */
object FlinkReadKafka2Hive {

    def main(args: Array[String]): Unit = {
        val settings = EnvironmentSettings.newInstance().inStreamingMode()
                .withConfiguration(setConf())
                .build() //读设置
        val tableEnv = TableEnvironment.create(settings) //获取table env
        setHive(tableEnv)

        /**读取kafka source*/
        getDataSource(tableEnv)

        tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE) //设置当前SQL语法为hive方言，该方言可以在整个上下文过程中来回切换
        /**创建hive表*/
        createHiveTable(tableEnv)

        tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT) //设置当前SQL语法为flink方言，
        /**将数据Sink到*/
        sinkData(tableEnv)

    }

    /**
      * @DESC: 设置Flink相关配置
      * */
    private def setConf(): Configuration ={
        val config = new Configuration() //设置checkpoint
        config.setString("execution.checkpointing.interval","10000")
        config.setString("state.backend", "filesystem")
        config.setString("state.checkpoints.dir","hdfs://192.168.211.106:8020/tmp/checkpoint/FlinkWithHive")
        config
    }

    /**
      * @DESC: 设置hive catalog
      * */
    private def setHive(tableEnv: TableEnvironment): Unit ={
        val name = "hive_test"  //取个catalog名字
        val database = "test"   //指定hive的database
        //val hiveConf = "./flink-coding/src/main/resources/" //指定hive-site.xml配置文件所在的地方

        /**读取hive配置，并生成hive的catalog对象*/
        val hive = new HiveCatalog(name,database, null) //hiveConf为null后，程序会自动到classpath下找hive-site.xml
        tableEnv.registerCatalog(name, hive) //将该catalog登记到Flink的table env环境中，这样flink就可以直接访问hive中的表

        tableEnv.useCatalog(name) //让当前的flink环境使用该catalog
    }

    /**
      * @DESC: 读取Kafka数据源
      * */
    private def getDataSource(tableEnv: TableEnvironment): Unit ={
        tableEnv.executeSql(
            """
              |drop table if exists test.kafkaTable;
            """.stripMargin)

        tableEnv.executeSql(
            """
              |Create table test.kafkaTable(
              |client_ip STRING,
              |domain STRING,
              |`time` STRING,
              |target_ip STRING,
              |rcode STRING,
              |query_type STRING,
              |authority_record STRING,
              |add_msg STRING,
              |dns_ip STRING
              |)
              |with(
              |'connector' = 'kafka',
              |'topic' = 'qianxin',
              |'properties.bootstrap.servers' = '192.168.211.107:6667',
              |'properties.group.id' = 'FlinkWithHive',
              |'scan.startup.mode' = 'latest-offset',
              |'value.format'='csv',                                 //确定数据源为文本格式
              |'value.csv.field-delimiter'='|'                      //确定文本数据源的分隔符
              |);
            """.stripMargin)
    }

    /**
      * @DESC: 创建hive目标数据表
      * */
    private def createHiveTable(tableEnv: TableEnvironment): Unit ={
        tableEnv.executeSql(
            """
              |CREATE TABLE if not exists test.kafka_flink_hive (
              |client_ip STRING,
              |domain STRING,
              |target_ip STRING,
              |rcode STRING,
              |query_type STRING,
              |authority_record STRING,
              |add_msg STRING,
              |dns_ip STRING
              |)
              |PARTITIONED BY (`time` STRING)
              |STORED AS textfile TBLPROPERTIES (
              |  'partition.time-extractor.timestamp-pattern'='$time',
              |  'sink.partition-commit.trigger'='partition-time',
              |  'sink.partition-commit.delay'='1 h',
              |  'sink.partition-commit.policy.kind'='metastore,success-file'
              |);
            """.stripMargin)
    }

    /**
      * @DESC: 将数据写入到目标表中
      * */
    private def sinkData(tableEnv: TableEnvironment): Unit ={
        tableEnv.executeSql(
            """
              |INSERT INTO test.kafka_flink_hive
              |SELECT client_ip,domain,target_ip,rcode,query_type,authority_record,add_msg,dns_ip,`time`
              |FROM test.kafkaTable;
            """.stripMargin)
    }

}
