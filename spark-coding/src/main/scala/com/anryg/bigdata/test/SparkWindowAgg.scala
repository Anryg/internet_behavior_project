package com.anryg.bigdata.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @DESC: Spark的Window类聚合用法
  * @Auther: Anryg
  * @Date: 2022/04/20 12:10
  */
object SparkWindowAgg {
    def main(args: Array[String]): Unit = {
        /**第一步：创建一个spark的配置类，用来告诉spark你得按照我的要求做事*/
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val sparkConf = new SparkConf()
        sparkConf.setAppName("小白学习大数据")
        sparkConf.setMaster("local[3]")



        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        val seq = Seq((1,"A"),(2,"B"),(3,"C"),(4,"B"),(5,"D"),(6,"B"),(7,"A"))
        val df = spark.createDataFrame(seq).toDF("id","word")

        import spark.implicits._  /**必须加上，否则不认识这个$符号*/
       val windowSpec = Window.partitionBy($"word")

        val window1 = Window.partitionBy("word").orderBy("id").rangeBetween(-1,3)

        import org.apache.spark.sql.functions._  /**引入spark内置函数*/
        val df1 = df.withColumn("new_col",avg($"id").over(/*不带参数就是所有行*/))


        val df2 = df.withColumn("new_col",sum($"id").over(window1)) /**一般基于一定的数据窗口*/


        val df3 = Seq((1, "a", "7"), (2, "a", ""), (3, "a", "6"), (4, "b", "2"), (9,"c", "8"), (7, "d", "5"), (5, "b",""), (6, "b", "1"))
         .toDF("id", "category", "score")
        val window = Window.partitionBy($"category").orderBy($"id").rowsBetween(-4, 4) /**如果是rangeBetween必须要orderBy，但是如果是rowsBetween的话，可以不用*/
        sparkConf.set("spark.sql.shuffle.partitions","11")
        val df4= df3.withColumn("score", when (column("score") === "", mean($"score".cast(IntegerType)) over window).otherwise(column("score")))
        df4.show()
        df3.createOrReplaceTempView("t1")
        val df6 = spark.sql("select id, category, case score when '' then (avg(score) over (rows between 4 preceding and 4 following)) else score end as score from t1")
        val df5 = df3.filter(row => row.getString(1) != "xx" )
        println(df3.rdd.partitions.size)
        //df6.show()
        println(df4.rdd.partitions.size)
    }
}











