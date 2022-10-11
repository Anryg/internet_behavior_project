package com.anryg.bigdata.test.data_skew

import java.util

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * @DESC: 一个数据倾斜的例子
  * @Auther: Anryg
  * @Date: 2022/10/10 17:00
  */
object DataSkew02 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("DataSkewTest02")/*.setMaster("local[*]")*/
        val spark = new SparkContext(conf)

        val rawRDD = spark.textFile(args(0)) //读取原始数据源

        val filteredRDD = rawRDD.filter(line => { /**筛选满足需要的数据，已到达数据倾斜的目的*/
            val array = line.split(",")
            val target_ip = array(3)
            target_ip.equals("106.38.176.185") || target_ip.equals("106.38.176.117") || target_ip.equals("106.38.176.118") || target_ip.equals("106.38.176.116")
        })

        val reducedRDD_01 = filteredRDD.map(line => {/**解决倾斜第一步：加盐操作将原本1个分区的数据扩大到100个分区*/
            val array = line.split(",")
            val target_ip = array(3)
            val client_ip = array(0)
            val index = client_ip.lastIndexOf(".")
            val subClientIP = client_ip.substring(0, index)//为了让后续聚合后的value数据量尽可能的少，只取ip的前段部分
            if (target_ip.equals("106.38.176.185")){/**针对特定倾斜的key进行加盐操作*/
                val saltNum = 99 //将原来的1个key增加到100个key
                val salt = new Random().nextInt(saltNum)
                (target_ip + "-" + salt,Array(subClientIP))
            }
            else (target_ip,Array(subClientIP))
        }).reduceByKey(_++_,103)//将Array中的元素进行合并,并确定分区数量

        val targetRDD_01 = reducedRDD_01.map(kv => {/**第二步：将各个分区中的数据进行初步统计，减少单个分区中value的大小*/
            val map = new util.HashMap[String,Int]()
            val target_ip = kv._1
            val clientIPArray = kv._2
            clientIPArray.foreach(clientIP => {//对clientIP进行统计
                if (map.containsKey(clientIP)) {
                    val sum = map.get(clientIP) + 1
                    map.put(clientIP,sum)
                }
                else map.put(clientIP,1)
            })
            (target_ip,map)
        })

        val reducedRDD_02 = targetRDD_01.map(kv => {/**第3步：对倾斜的数据进行减盐操作，将分区数从100减到10*/
            val targetIPWithSalt01 = kv._1
            val clientIPMap = kv._2
            if (targetIPWithSalt01.startsWith("106.38.176.185")){
                val targetIP = targetIPWithSalt01.split("-")(0)
                val saltNum = 9 //将原来的100个分区减少到10个分区
                val salt = new Random().nextInt(saltNum)
                (targetIP + "-" + salt,clientIPMap)
            }
            else kv
        }).reduceByKey((map1,map2) => { /**合并2个map中的元素，key相同则value值相加*/
            val map3 = new util.HashMap[String,Int](map1)
            map2.forEach((key,value) => {
                map3.merge(key, value, (v1,v2) => v1 + v2) //将map1和map2中的结果merge到map3中，相同的key，则value相加
            })
            map3
        },13)//调整分区数量

        val finalRDD = reducedRDD_02.map(kv => {/**第4步：继续减盐，将原本10个分区数的数据恢复到1个*/
            val targetIPWithSalt01 = kv._1
            val clientIPMap = kv._2
            if (targetIPWithSalt01.startsWith("106.38.176.185")){
                val targetIP = targetIPWithSalt01.split("-")(0)
                (targetIP,clientIPMap)//彻底将盐去掉
            }
            else kv
        }).reduceByKey((map1,map2) => { /**合并2个map中的元素，key相同则value值相加*/
            val map3 = new util.HashMap[String,Int](map1)
            map2.forEach((key,value) => {
                map3.merge(key, value, (v1,v2) => v1 + v2)
            })
            map3
        },4)//调整分区数量

        finalRDD.saveAsTextFile(args(1))
    }
}
