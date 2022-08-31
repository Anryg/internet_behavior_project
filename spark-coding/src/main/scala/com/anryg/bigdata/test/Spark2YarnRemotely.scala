package com.anryg.bigdata.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @DESC:
  * @Auther: Anryg
  * @Date: 2022/5/17 11:28
  */
object Spark2YarnRemotely {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        val spark = SparkSession.builder().config(conf)
                .master("yarn")
                .config("spark.yarn.jars","F:\\Project\\demo\\spark-coding\\target\\spark-coding-1.0-SNAPSHOT-with-dependencies.jar")//提供yarn运行时的依赖
                .config("spark.hadoop.fs.defaultFS","hdfs://hdp01.pcl-test.com:8020")/**告诉程序，用来上传当前运行的jar包*/
                .config("spark.hadoop.yarn.resourcemanager.address","hdp01.pcl-test.com:8050") /**这里需要指定yarn地址，直接放yarn-site.xml无法连接*/
                .config("spark.driver.host","192.168.25.220")
                //.enableHiveSupport()
                .getOrCreate()
    }
}
