package com.anryg.bigdata.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @DESC: 小白学大数据编程之spark数据处理程序——最简spark程序演示
  * @Auther: Anryg
  * @Date: 2022/04/20 12:10
  */
object BigdataDemo_5 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        val spark = SparkSession.builder().config(conf)
                .master("local").getOrCreate()

        val rawDF = spark.read.option("header","true")/**需要显性告诉spark其schema*/
                .csv("C:\\Users\\Anryg\\Documents\\WeChat Files\\zhangqinhe001\\FileStorage\\MsgAttach\\1a19725c3ffd9a718b39ee8abd414833\\File\\2022-06\\qnr.csv")
        rawDF.printSchema()
        rawDF.toDF()
        rawDF.groupBy("Date").count().show()
        rawDF.createOrReplaceTempView("t1")
        /**根据出游人规模统计*/
        val peopleAgg = spark.sql("select People,count(*) as count from t1 group by People sort by count desc")
        peopleAgg.show()
        /**根据出游费用统计*/
        val feeAgg = spark.sql("select Fee,count(*) as count from t1 group by Fee sort by count desc")
        feeAgg.show()
        /**根据出游日期统计*/
        val dateAgg = spark.sql("select Date,count(*) as count from t1 group by Date sort by count desc")
        dateAgg.show()


    }
}











