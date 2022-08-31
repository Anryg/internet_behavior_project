package com.anryg.bigdata.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @DESC: 小白学大数据编程之spark数据处理程序——aggregate
  * @Auther: Anryg
  * @Date: 2022/04/11 12:10
  */
object BigdataDemo {
    def main(args: Array[String]): Unit = {
        /**第一步：创建一个spark的配置类，用来告诉spark你得按照我的要求做事*/
        val sparkConf = new SparkConf()
        sparkConf.setAppName("小白学习大数据")
        sparkConf.setMaster("local[3]")//启用3个线程
        /**第二步：创建spark的上下文，什么叫上下文，就是这个程序的boss，大管家*/
        val sparkContext = new SparkContext(sparkConf)//把上面配置加入到这个上下文中
        /*********************上面为程序的环境部分，即不动的部分*************/

        /**第三步：创建数据源*/
        val seq = Seq(1,2,3,4,5,6)
        /**第四步：将数据源读到进程内存中*/
        val rdd1 = sparkContext.parallelize(seq,3)//将数据分为3个分区

        /**第五步：通过添加基础数(10,100)进行聚合统计*/
        val result = rdd1.aggregate((10,100))(
            (zeroValue, num) =>
                (zeroValue._1 + num, zeroValue._2 + num),//分区内部的聚合逻辑
            (part1, part2) =>
                (part1._1 + part2._1, part1._2 + part2._2)//分区之间的聚合逻辑
        )
        /**第六步：打印聚合后的数据*/
        println("分区为3时的聚合结果为：" + result)
    }
}











