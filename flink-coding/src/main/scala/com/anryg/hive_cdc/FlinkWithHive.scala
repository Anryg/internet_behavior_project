package com.anryg.hive_cdc

import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf

/**
  * @DESC: Flink连接hive
  * @Auther: Anryg
  * @Date: 2022/12/19 10:36
  */
object FlinkWithHive {

    def main(args: Array[String]): Unit = {
        val settings = EnvironmentSettings.newInstance().inStreamingMode().build() //读取默认设置
        val tableEnv = TableEnvironment.create(settings) //获取table env
        tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE) //设置当前SQL语法为hive方言，该方言可以在整个上下文过程中来回切换
        setHive(tableEnv)

        /**查看当前database有哪些表*/
        tableEnv.executeSql(
            """
              |SHOW tables;
            """.stripMargin).print()


        /**将数据Sink到*/
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
}
