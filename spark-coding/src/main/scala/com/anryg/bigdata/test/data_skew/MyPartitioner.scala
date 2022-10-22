package com.anryg.bigdata.test.data_skew

import org.apache.spark.Partitioner

/**
  * @DESC: 实现自定义的分区策略
  * @Auther: Anryg
  * @Date: 2022/10/13 09:52
  */
class MyPartitioner(partitionNum: Int) extends Partitioner{
    override def numPartitions: Int = partitionNum  //确定总分区数量

    override def getPartition(key: Any): Int = {//确定数据进入分区的具体策略
        val keyStr = key.toString
        val keyTag = keyStr.substring(keyStr.length - 1, keyStr.length)
        keyTag.toInt % partitionNum
    }
}
