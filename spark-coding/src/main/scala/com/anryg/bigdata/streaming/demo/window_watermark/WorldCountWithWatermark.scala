package com.anryg.bigdata.streaming.demo.window_watermark

import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
  * @DESC: 用时间窗口和watermark来进行client_ip的worldcount统计
  * @Auther: Anryg
  * @Date: 2022/11/30 10:04
  */
object WorldCountWithWatermark {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("WorldCountWithWatermark").setMaster("local")
        val spark = SparkSession.builder()
                .config(conf)
                .getOrCreate()
        Logger.getLogger("org.apache").setLevel(Level.WARN) //减少INFO日志的输出

        val rawDF = spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.211.107:6667")
                .option("subscribe", "qianxin")
                //.option("group.id","test9999") /**不再用该方式来绑定offset，而是每个程序有个唯一的id，该id跟checkpointLocation绑定，虽然group.id属性在运行中依然保留，但是不再跟offset绑定*/
                .option("failOnDataLoss",false)
                .option("fetchOffset.numRetries",3)
                //.option("maxOffsetsPerTrigger",Integer.MAX_VALUE)/**用于限流，限定每个批次取的数据条数，确定写入HDFS单个文件的条数*/
                .option("startingOffsets","latest")
                .load()

        import spark.implicits._
        val df1 = rawDF.selectExpr("CAST(value AS string)")
                .map(row =>{
                    val line = row.getAs[String]("value")
                    val fieldArray:Array[String] = line.split("\\|")
                    fieldArray
                })
                .filter(_.length == 9) //确定字段数必须为9个
                .filter(_(1).endsWith("com")) //防止数量太大，对访问的网站做的一点限制
               .map(array =>{
                    val sdf = new SimpleDateFormat("yyyyMMddhhmmss").parse(array(2))
                    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(sdf)
                    (array(0), Timestamp.valueOf(time)) //因为time这个字段要作为watermark字段，它必须是yyyy-MM-dd HH:mm:ss样式的Timestamp类型
                })
                .toDF("client_ip", "time") //添加schema

        import org.apache.spark.sql.functions._  /**引入spark内置函数*/

        val df2 = df1.withWatermark("time", "10 seconds") //一般需要跟window一起配合使用
                .groupBy(window($"time","2 minutes","30 seconds"), $"client_ip") //确定具体字段，以及对应的聚合时间窗口，和滑动窗口
                .count()
                .orderBy($"count".desc)
                .limit(10)

        val query = df2.writeStream
                .format("console") //打印到控制台
                .option("truncate", false) //将结果的内容完整输出，默认会砍掉内容过长的部分
                .option("numRows",30) //一次最多打印多少行，默认20行
                .option("checkpointLocation","hdfs://192.168.211.106:8020/tmp/offset/WorldCountWithWatermark") //确定checkpoint目录
                //.outputMode(OutputMode.Update())//不支持排序的结果
                .outputMode(OutputMode.Complete()) //确定输出模式，默认为Append
                .start()

        query.awaitTermination()
    }

}