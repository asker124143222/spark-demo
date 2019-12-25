package com.home.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: xu.dm
  * @Date: 2019/12/25 10:26
  * @Version: 1.0
  * @Description: TODO
  **/
object Ex_kafkaSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark streaming wordcount")
    //优雅的停止SparkStreaming而不丢失数据
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    //环境对象，设置采集周期
    val scc: StreamingContext = new StreamingContext(conf, Seconds(30))
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

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      scc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](
        topics,
        kafkaParams
      )
    )

    kafkaStream.foreachRDD(rdd => {
      val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val maped: RDD[(String, String)] = rdd.map(record => (record.key,record.value))
      //计算逻辑
      maped.foreach(println)
      //循环输出
      for(o <- offsetRange){
        println(s"${o.topic}  ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    })

    val words: DStream[String] = kafkaStream.flatMap(t=>t.value().split(" "))

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
