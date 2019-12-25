package com.home.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: xu.dm
  * @Date: 2019/12/24 10:22
  * @Version: 1.0
  * @Description: spark-streaming
  **/
object WordCountStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark streaming wordcount")
    //优雅的停止SparkStreaming而不丢失数据
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")

    //环境对象，设置采集周期
    val scc: StreamingContext = new StreamingContext(conf,Seconds(30))
    // TODO: 可以通过ssc.sparkContext 来访问SparkContext或者通过已经存在的SparkContext来创建StreamingContext

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val socketStream: ReceiverInputDStream[String] = scc.socketTextStream("vmhome10.com",9999)

    val words: DStream[String] = socketStream.flatMap(_.split(" "))

    val pairs = words.map(word => (word,1))

    val wordCounts : DStream[(String, Int)] = pairs.reduceByKey(_+_)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print

    // Start the computation
    // 通过 streamingContext.start()来启动消息采集和处理
    scc.start()

    // Wait for the computation to terminate
    // 通过streamingContext.stop()来手动终止处理程序
    scc.awaitTermination()

  }
}
