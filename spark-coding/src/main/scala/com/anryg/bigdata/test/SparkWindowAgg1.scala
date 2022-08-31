package com.anryg.bigdata.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.FloatType


/**
  * @DESC: Spark的Window类聚合用法
  * @Auther: Anryg
  * @Date: 2022/04/20 12:10
  */
object SparkWindowAgg1 {
    def main(args: Array[String]): Unit = {
        /**第一步：创建一个spark的配置类，用来告诉spark你得按照我的要求做事*/
        val sparkConf = new SparkConf().setAppName("一道面试题").setMaster("local[3]")

        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._  /**引入隐式转换对象*/
        import org.apache.spark.sql.functions._  /**引入spark内置函数*/

        val df1 = Seq((1, "a", "71"), (2, "a", ""), (3, "a", "66"), (4, "c", "82"),
            (9,"c", ""), (7, "d", "55"), (5, "b",""), (6, "b", "91"),(8,"d","49"))
         .toDF("id", "category", "score") //定义数据源，这里只是举个栗子

        val window = Window.partitionBy(column("category")).orderBy($"id").rowsBetween(-4, 4) /**定义窗口对象，确定应用的数据窗口*/
        val df2 = df1.withColumn("score",
            when (column("score") === "", avg($"score".cast(FloatType)) over window)
                    .otherwise(column("score").cast(FloatType)))/**通过对空值字段进行基于窗口期数据求均值，并对空值进行替换*/
        df2.show()
        println(df1.rdd.partitions.size)
        println(df2.rdd.partitions.size)
    }
}











