package com.anryg.window_and_watermark

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Locale

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.configuration.MemorySize
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * @DESC: 读取kafka数据，从DataStream到HDFS
  * @Auther: Anryg
  * @Date: 2022/8/14 19:08
  */
object FlinkDSFromKafkaWithWatermark {

    private final val hdfsPrefix = "hdfs://192.168.211.106:8020"

    def main(args: Array[String]): Unit = {
        //获取流任务的环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
                .enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE) //打开checkpoint功能

        env.getCheckpointConfig.setCheckpointStorage(hdfsPrefix + "/tmp/flink_checkpoint/FlinkDSFromKafkaWithWatermark") //设置checkpoint的hdfs目录
        env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) //设置checkpoint记录的保留策略

        val kafkaSource = KafkaSource.builder()  //获取kafka数据源
                .setBootstrapServers("192.168.211.107:6667")
                .setTopics("qianxin")
                .setGroupId("FlinkDSFromKafkaWithWatermark")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build()

        import org.apache.flink.streaming.api.scala._  //引入隐私转换函数

        val kafkaDS = env.fromSource(kafkaSource,
            WatermarkStrategy.noWatermarks()
            ,"kafka-data") //读取数据源生成DataStream对象

        val targetDS = kafkaDS.map(line => { //对数据源做简单的ETL处理
            line.split("\\|")
        }).filter(_.length == 9).filter(_(1).endsWith("com"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofHours(10)) //指定watermark
                        .withTimestampAssigner(new SerializableTimestampAssigner[Array[String]] {
                            override def extractTimestamp(element: Array[String], recordTimestamp: Long): Long = {
                                val sdf = new SimpleDateFormat("yyyyMMddhhmmss")
                                sdf.parse(element(2)).getTime  //指定的watermark字段必须是Long类型的时间戳
                            }
                        }))
                .map(array => (array(0), 1))
                .keyBy(kv => kv._1) //根据client_ip聚合
                .window(SlidingProcessingTimeWindows.of(Time.minutes(2), Time.seconds(30)))  //指定window，这里的window assigner必须是基于Process Time而不是Event Time，因为数据时间跟当前真实时间相差有点多
                .sum(1)

        targetDS.print() //打印结果

        env.execute("FlinkDSFromKafkaWithWatermark") //启动任务
    }
}
