package com.anryg.bigdata.hive

import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession

/**
  * @DESC: 必须是非ACID表,否则读取为空表，但不报错
  * @Auther: Anryg
  * @Date: 2022/4/8 16:30
  */
/*object Spark3ConnectHive3 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("connect_hive")
        val sparkSession = SparkSession.builder().config(conf)
                //.config("spark.sql.warehouse.dir","hdfs://192.168.211.106:8020/warehouse/tablespace/managed/hive")
                //.config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://hdp01.pcl-test.com:2181,hdp03.pcl-test.com:2181,hdp02.pcl-test.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")
                //.config("spark.datasource.hive.warehouse.metastoreUri","thrift://hdp01.pcl-test.com:9083")
                //.config("spark.sql.hive.strict.managed.tables", false)
                .enableHiveSupport()
                .getOrCreate()

        val result = sparkSession.sql("select * from xas.as_bgp_bak limit 3")
        result.show()

        sparkSession.close()
        sparkSession.stop()
    }
}*/


/*val hive = HiveWarehouseSession.session(sparkSession).build()//获取HWC对象
        val result = hive.executeQuery("select * from doi_data limit 2")//查询hive表数据
        result.show()*/