package com.anryg.bigdata.test

import org.apache.spark
import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @DESC: 小白学大数据编程之spark数据处理程序——最简spark程序演示
  * @Auther: Anryg
  * @Date: 2022/04/20 12:10
  */
object BigdataDemo_3 {
    def main(args: Array[String]): Unit = {
        /**第一步：创建一个spark的配置类，用来告诉spark你得按照我的要求做事*/
        val sparkConf = new SparkConf()
        sparkConf.setAppName("broadcast_test")
        sparkConf.setMaster("local[10]")
        /**第二步：创建spark的上下文，什么叫上下文，就是这个程序的boss，大管家*/
        val sparkContext = new SparkContext(sparkConf)//把配置加入到这个上下文中
        val accumulator = sparkContext.longAccumulator("数值累计器")
        val accumulator2 = sparkContext.collectionAccumulator[String]("集合累加器")
        /**定义一个movie主题的map*/
        //val map = Map(("001","movie001"),("002","movie002"),("003","movie003"),("004","movie004"))
        /**添加到广播变量*/
        //val movieBroadcast = sparkContext.broadcast(map)
        /**第三步：创建数据源*/
        val seq = Seq("a","b","c","","d","","e","f","g","h","i","","")
        /**第四步：将数据源读到进程内存中*/
        val rdd1 = sparkContext.parallelize(seq,10)
        rdd1.coalesce(3,false)
        /**第五步：将rdd中的数据进行打印*/
       /* rdd1.foreach(data => {
            if (data != "") accumulator.add(1)
            //val movieMap = movieBroadcast.value/**读取广播变量的对象*/
            println(data)
        })*/
        rdd1.foreachPartition(iter => {
            while (iter.hasNext) {
                val data = iter.next()
                if (data != "") {
                    accumulator.add(1)
                    accumulator2.add(data)
                    println("xxxxx" + accumulator.value)
                }
                else accumulator.add(5)
            }
        })
        println("The count is: " + accumulator.count)
        println("The sum is: " + accumulator.sum)
        println("The value is: " + accumulator.value)
        println("The avg is: " + accumulator.avg)
        println("The list is: " + accumulator2.value)
    }
}











