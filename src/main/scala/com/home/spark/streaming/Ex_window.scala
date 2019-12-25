package com.home.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: xu.dm
  * @Date: 2019/12/25 16:36
  * @Version: 1.0
  * @Description:
  *  TODO 窗口操作，即把几个批次的数据整合到一个窗口里计算，并且窗口根据步长不断滑动
  *   所有基于窗口的操作都需要两个参数，分别为窗口时长以及滑动步长，两者都必须是 StreamContext 的批次间隔的整数倍。
  *   窗口时长控制每次计算最近的多少个批次的数据，其实就是最近的 windowDuration/batchInterval 个批次。
  *   如果有一个以 10 秒为批次间隔的源 DStream，要创建一个最近 30 秒的时间窗口(即最近 3 个批次)，就应当把 windowDuration 设为 30 秒。
  *   而滑动步长的默认值与批次间隔相等，用来控制对新的 DStream 进行计算的间隔。如果源 DStream 批次间隔为 10 秒，
  *   并且我们只希望每两个批次计算一次窗口结果， 就应该把滑动步长设置为 20 秒。
  **/
object Ex_window {
  def main(args: Array[String]): Unit = {
    val ints = List(1,2,3,4,5,6,7,8)

    //scala的滑动窗口
    val iterator: Iterator[List[Int]] = ints.sliding(3,3)

    for(list<-iterator){
      println(list.mkString(","))
    }


    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark streaming wordcount")
    //优雅的停止SparkStreaming而不丢失数据
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    //环境对象，设置采集周期
    val scc: StreamingContext = new StreamingContext(conf, Seconds(5))
    // TODO: 可以通过ssc.sparkContext 来访问SparkContext或者通过已经存在的SparkContext来创建StreamingContext

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.44.10:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array("test")

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      scc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        topics,
        kafkaParams
      )
    )


    //采集周期为5秒，窗口为15秒（包含三个采集批次），滑动步长为5秒，即每个批次滑动一次。
    val words: DStream[String] = kafkaStream.flatMap(t=>t.value().split(" ")).window(Seconds(15),Seconds(5))


    //    val words: DStream[String] = socketStream.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))

    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

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
