package com.anryg.bigdata.test

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @DESC: 小白学大数据编程之spark数据处理程序——mapPartitionsWithIndex
  * @Auther: Anryg
  * @Date: 2022/04/19 12:10
  */
object BigdataDemo_2 {
    def main(args: Array[String]): Unit = {
        /**第一步：创建一个spark的配置类，用来告诉spark你得按照我的要求做事*/
        val sparkConf = new SparkConf()
        sparkConf.setAppName("小白学习大数据")
        sparkConf.setMaster("local[3]")//启用3个线程
        /**第二步：创建spark的上下文，什么叫上下文，就是这个程序的boss，大管家*/
        val sparkContext = new SparkContext(sparkConf)//把上面配置加入到这个上下文中
        /*********************上面为程序的环境部分，即不动的部分*************/
        /**第三步：创建数据源*/
        val seq = Seq("a","b","c","a","c","d","a")
        /**第四步：将数据源读到进程内存中*/
        val rdd1 = sparkContext.parallelize(seq,3)//将数据分为3个分区
        /**第五步：通过分区编号 将相同编号的数据作为一个kv放到Map中*/
        val rdd2 = rdd1.mapPartitionsWithIndex((index,iterator) => {
            val map = mutable.Map[Int,Iterator[String]]()
            map.put(index,iterator)//相同index的数据放在同一个迭代器
            map.toIterator//将Map对象转化为迭代器
        })
        /**第六步：打印分组后的数据*/
        rdd2.foreach(kv =>{
            val index = kv._1//获取分区编号
            val iterator = kv._2//获取当前分区数据的集合
            println(index + "_" + iterator.toList)//打印当前分区与当前分区的数据
        })
    }
}











