package com.anryg.bigdata.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.FloatType


/**
  * @DESC: Spark的Window类聚合用法
  * @Auther: Anryg
  * @Date: 2022/04/20 12:10
  */
object ReadHDFS {
    def main(args: Array[String]): Unit = {
        /**第一步：创建一个spark的配置类，用来告诉spark你得按照我的要求做事*/
        val sparkConf = new SparkConf().setAppName("一道面试题").setMaster("local[3]")
        //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._  /**引入隐式转换对象*/
        import org.apache.spark.sql.functions._  /**引入spark内置函数*/

        val rawDF = spark.read.option("header",true)
                .csv("hdfs://192.168.211.106:8020/DATA/qianxin/part-00000-f5bdd8d9-bde9-4c47-a50e-ace37f891726-c000.csv").cache()

        rawDF.show()
        //println(rawDF.rdd.partitions.size)
        //rawDF.select(col("time")).distinct().show()
        val window = Window/*.partitionBy(column("category"))*/.rowsBetween(-4, 4)
        val df2 = rawDF.withColumn("xxx",
            when (column("time") =!= "", avg($"time".cast(FloatType)) over window)
                    .otherwise(column("time").cast(FloatType))).cache()
        df2.show(100)

        println(df2.rdd.partitions.size)
    }
}











