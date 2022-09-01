package com.anryg.bigdata.streaming

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, OutputMode}

/**
  * @DESC: 对数据处理的共性部分进行提取
  * @Auther: Anryg
  * @Date: 2022/8/31 17:50
  */
trait StreamingProcessHelper[Any] {

    /**
      * @DESC: 以流的方式获取数据source
      * @param sparkSession:
      * @param dataSource: 数据源形式，比如kafka
      * @param config: 对流式数据源的配置
      * */
    def getStreamingReader(sparkSession:SparkSession, dataSource:String, config:Map[String,String]): DataStreamReader ={
        val streamingReader = sparkSession.readStream
                .format(dataSource)
                .options(config)
        streamingReader
    }

    /**
      * @DESC: 以流方式对数据进行sink
      * @param dataSet: 处理完成的结果数据集
      * @param outputMode: sink的类型：Complete、append、update
      * @param config: 对sink对象的配置
      * */
    def getStreamingWriter(dataSet:DataFrame, outputMode:OutputMode, outputFormat:String, config:Map[String,String]): DataStreamWriter[Row] ={
        val streamingWriter = dataSet.writeStream
                .format(outputFormat)
                .outputMode(outputMode)
                .options(config)
        streamingWriter
    }

}
