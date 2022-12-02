package com.anryg.window_and_watermark

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Duration
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment



/**
  * @DESC: 读取kafka数据，从DataStream API转为Table API，并利用watermark
  * @Auther: Anryg
  * @Date: 2022/8/14 19:08
  */
object FlinkTBFromKafkaWithWatermark {
    private final val hdfsPrefix = "hdfs://192.168.211.106:8020"//HDFS地址前缀

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment //获取流环境变量
                                            .enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE) //打开checkpoint功能

        val tableEnv = StreamTableEnvironment.create(env)  //创建Table环境变量
        env.getCheckpointConfig.setCheckpointStorage(hdfsPrefix + "/tmp/flink_checkpoint/FlinkTBFromKafkaWithWatermark") //设置checkpoint的hdfs目录
        env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) //设置checkpoint记录的保留策略

        val kafkaSource = KafkaSource.builder()
                        .setBootstrapServers("192.168.211.107:6667")
                        .setTopics("qianxin")
                        .setGroupId("FlinkTBFromKafkaWithWatermark")
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build()
        val kafkaDS = env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"kafka-data")
        val targetDS = kafkaDS.map(_.split("\\|"))
                                .filter(_.length == 9)
                                .filter(_(1).endsWith("com"))
                                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))  //给业务字段分配watermark
                                                                                .withTimestampAssigner(new SerializableTimestampAssigner[Array[String]] {
                                                                                    override def extractTimestamp(element: Array[String], recordTimestamp: Long): Long = { //实现watermark字段的分配
                                                                                        val sdf = new SimpleDateFormat("yyyyMMddhhmmss")
                                                                                        sdf.parse(element(2)).getTime
                                                                                    }
                                }))
                                .map(array => (array(0), array(2)))
                                .map(kv => {
                                    val date = kv._2
                                    val sdf = new SimpleDateFormat("yyyyMMddhhmmss").parse(date)
                                    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(sdf)
                                    (kv._1, Timestamp.valueOf(time)) //将时间转为要求的Time Attributes 也就是Timestamp类型
                                })

        import org.apache.flink.table.api._  //加入隐式转换，否则下面的$无法识别

        val targetTable = tableEnv.fromDataStream(targetDS)
                                .as("client_ip", "time") //添加schema
                                .window(
                                    Slide  over 1.minute every 30.seconds() on $"time" as $"w"  //加入window
                                )
                                .groupBy($"client_ip", $"w")
                                .select(
                                    $"client_ip",
                                    $"w".start(), //时间窗口的开始时间
                                    $"w".end(),  //时间窗口的解释时间
                                    $"client_ip".count() as "count"
                                )
                                .orderBy($"count")
                                .limit(10)
        targetTable.execute().print()
    }
}
