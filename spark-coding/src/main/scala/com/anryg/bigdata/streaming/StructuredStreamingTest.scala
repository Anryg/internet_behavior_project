/*
package com.anryg.bigdata.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

import scala.collection.mutable

/**
  * @DESC: 测试daemon,注意如果在Windows下测试运行，需要在Windows上设置HADOOP_HOME环境变量,且需要在$HADOOP_HOME/bin目录下放置hadoop.dll和winutils两个文件
  * @Auther: Anryg
  * @Date: 2021/3/1 11:09
  */
object StructuredStreamingTest {


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("Structured streaming test")
        //val conf = SparkConfFactory.newSparkConf().setMaster("local[2]").setAppName("Structured streaming test")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        val rawDF = spark.readStream.format("socket") /**如果结果输出为complete模式，原始DF不能直接作为结果输出，必须经过聚合处理才可以，否则会有如下报错*/
            /*Exception in thread "main" org.apache.spark.sql.AnalysisException:
            Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets;;*/
            .option("host","192.168.211.106")
            .option("port",9998)
            .load()

        import spark.implicits._



        val xxx = rawDF.as[String].foreachPartition(iter => {
            while (iter.hasNext) println(iter.next())
        })
        /*mapPartitions(iterator => {
            val array = new mutable.ArrayBuffer[String]
            while (iterator.hasNext){
                val next = iterator.next()
                array.+=(next)
            }
            array.toIterator
        })*/

        val query = rawDF.writeStream
            .outputMode(OutputMode.Append())
            .format("console")
            .start()

        query.awaitTermination()


        //rawDF.take(10).foreach(println(_))


    }

}
*/
