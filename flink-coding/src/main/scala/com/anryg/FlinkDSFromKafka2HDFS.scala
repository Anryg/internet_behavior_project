package com.anryg

import java.time.Duration

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.configuration.MemorySize
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.CheckpointStorage
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
  * @DESC: 读取kafka数据，从DataStream到HDFS
  * @Auther: Anryg
  * @Date: 2022/8/14 19:08
  */
object FlinkDSFromKafka2HDFS {

    private final val hdfsPrefix = "hdfs://192.168.211.106:8020"

    def main(args: Array[String]): Unit = {
        //获取流任务的环境变量
        val env = StreamExecutionEnvironment.getExecutionEnvironment
                .enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE) //打开checkpoint功能

        env.getCheckpointConfig.setCheckpointStorage(hdfsPrefix + "/tmp/flink_checkpoint/FlinkDSFromKafka2HDFS") //设置checkpoint的hdfs目录

        val kafkaSource = KafkaSource.builder()  //获取kafka数据源
                .setBootstrapServers("192.168.211.107:6667")
                .setTopics("qianxin")
                .setGroupId("FlinkDSFromKafka2HDFS2")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build()

        import org.apache.flink.streaming.api.scala._  //引入隐私转换函数
        val kafkaDS = env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"kafka-data") //读取数据源生成DataStream对象

        val targetDS = kafkaDS.map(line => { //对数据源做简单的ETL处理
            line.split("\\|")
        }).filter(_.length == 9).map(array => (array(0),array(1),array(2),array(3),array(4),array(5),array(6),array(7),array(8)))

        /**基于flink1.14之后新的，文件系统的sink策略，跟官网提供的不一致，有坑*/
        val hdfsSink2 = StreamingFileSink.forRowFormat(new Path(hdfsPrefix + "/tmp/flink_sink3"),
            new SimpleStringEncoder[(String,String,String,String,String,String,String,String,String)]("UTF-8"))
                //.withBucketAssigner(new DateTimeBucketAssigner) /**默认基于时间分配器*/
                .withRollingPolicy( //设置文件的滚动策略，也就是分文件策略，也可以同时设置文件的命名规则，这里暂时用默认
            DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofSeconds(300)) //文件滚动间隔，设为5分钟，即每5分钟生成一个新文件
                    .withInactivityInterval(Duration.ofSeconds(20)) //空闲间隔时间，也就是当前文件有多久没有写入数据，则进行滚动
                    .withMaxPartSize(MemorySize.ofMebiBytes(800)) //单个文件的最大文件大小，设置为500MB
                    .build()).build()

        targetDS.addSink(hdfsSink2) //目标DataStream添加sink策略

        env.execute("FlinkDSFromKafka2HDFS") //启动任务
    }
}
