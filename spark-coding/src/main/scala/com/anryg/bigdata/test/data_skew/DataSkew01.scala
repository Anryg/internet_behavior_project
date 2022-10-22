package com.anryg.bigdata.test.data_skew

import java.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @DESC: 一个数据倾斜的例子
  * @Auther: Anryg
  * @Date: 2022/10/10 17:00
  */
object DataSkew01 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("DataSkewTest01")/*.setMaster("local[*]")*/
        val spark = new SparkContext(conf)

        val rawRDD = spark.textFile(args(0))//读取数据源

        val filteredRDD = rawRDD.filter(line => { /**筛选满足需要的数据，已到达数据倾斜的目的*/
            val array = line.split(",")
            val target_ip = array(3)
            target_ip.equals("106.38.176.185") || target_ip.equals("106.38.176.117") || target_ip.equals("106.38.176.118") || target_ip.equals("106.38.176.116")
        })

        val reducedRDD = filteredRDD.map(line => {/**根据目的ip进行汇总，将访问同一个目的ip的所有客户端ip进行汇总*/
            val array = line.split(",")
            val target_ip = array(3)
            val client_ip = array(0)
            val index = client_ip.lastIndexOf(".")
            val subClientIP = client_ip.substring(0, index) //为了让后续聚合后的value数据量尽可能的少，只取ip的前段部分
            (target_ip,Array(subClientIP))
        }).reduceByKey(new MyPartitioner(4), _++_)//将Array中的元素进行合并

        val targetRDD = reducedRDD.map(kv => {/**将访问同一个目的ip的客户端，再次根据客户端ip进行进一步统计*/
            val map = new util.HashMap[String,Int]()
            val target_ip = kv._1
            val clientIPArray = kv._2
            clientIPArray.foreach(clientIP => {
                if (map.containsKey(clientIP)) {
                    val sum = map.get(clientIP) + 1
                    map.put(clientIP,sum)
                }
                else map.put(clientIP,1)
            })
            (target_ip,map)
        })

        targetRDD.saveAsTextFile("/tmp/DataSkew01") //结果数据保存目录
    }
}
