package com.anryg.bigdata.test

import org.apache.spark.{SparkConf, SparkContext}


/**
  * @DESC: 小白学大数据编程之spark数据处理程序——最简spark程序演示
  * @Auther: Anryg
  * @Date: 2022/04/20 12:10
  */
object BigdataDemo_4 {
    def main(args: Array[String]): Unit = {
        /**第一步：创建一个spark的配置类，用来告诉spark你得按照我的要求做事*/
        val sparkConf = new SparkConf()
        sparkConf.setAppName("小白学习大数据")
        sparkConf.setMaster("local")
        /**第二步：创建spark的上下文，什么叫上下文，就是这个程序的boss，大管家*/
        val sparkContext = new SparkContext(sparkConf)//把配置加入到这个上下文中
        /*********************上面为程序的环境部分，即不动的部分*************/
        /**第三步：创建数据源*/
        val rawRdd = sparkContext.textFile("C:\\Users\\Anryg\\Documents\\WeChat Files\\zhangqinhe001\\FileStorage\\MsgAttach\\1a19725c3ffd9a718b39ee8abd414833\\File\\2022-06\\qnr.csv")
        val arrayRdd = rawRdd.map(line => line.split(","))
        println("The total line is: " + rawRdd.count())
        println("The bad line is: " + arrayRdd.filter(array => array.length != 11).count())
        /*val result = arrayRdd.map(array => {
            (array(5),1)
        }).reduceByKey(_+_)

        result.foreach(println(_))*/

    }
}











