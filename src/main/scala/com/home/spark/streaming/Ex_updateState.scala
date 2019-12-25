package com.home.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}

/**
  * @Author: xu.dm
  * @Date: 2019/12/25 14:54
  * @Version: 1.0
  * @Description:
  * TODO 更新数据状态，把每个采集周期的数据进行整合业务处理
  *   无状态操作，即操作的数据都是每个批次内的数据（一个采集周期）
  *   状态操作，即操作从启动到当前的所有采集周期内的数据（跨批次操作）
  *   UpdateStateByKey原语用于记录历史记录，有时，我们需要在 DStream 中跨批次维护状态(例如流计算中累加wordcount)。
  *   针对这种情况，updateStateByKey() 为我们提供了对一个状态变量的访问，用于键值对形式的 DStream。
  *   给定一个由(键，事件)对构成的 DStream，并传递一个指定如何根据新的事件 更新每个键对应状态的函数，
  *   它可以构建出一个新的 DStream，其内部数据为(键，状态) 对。
  *   updateStateByKey() 的结果会是一个新的 DStream，其内部的 RDD 序列是由每个时间区间对应的(键，状态)对组成的。
  *   updateStateByKey操作使得我们可以在用新信息进行更新时保持任意的状态。为使用这个功能，你需要做下面两步：
  *   1. 定义状态，状态可以是一个任意的数据类型。
  *   2. 定义状态更新函数，用此函数阐明如何使用之前的状态和来自输入流的新值对状态进行更新。
  *   使用updateStateByKey需要对检查点目录进行配置，会使用检查点来保存状态
  **/
object Ex_updateState {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark streaming wordcount")
    //优雅的停止SparkStreaming而不丢失数据
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    //环境对象，设置采集周期
    val scc: StreamingContext = new StreamingContext(conf, Seconds(10))
    // TODO: 可以通过ssc.sparkContext 来访问SparkContext或者通过已经存在的SparkContext来创建StreamingContext

    //设置检查点目录
    scc.sparkContext.setCheckpointDir("checkpoint")

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
      ConsumerStrategies.Subscribe[String, String](
        topics,
        kafkaParams
      )
    )

    kafkaStream.foreachRDD(rdd => {
      val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val maped: RDD[(String, String)] = rdd.map(record => (record.key, record.value))
      //计算逻辑
      maped.foreach(println)
      //循环输出
      for (o <- offsetRange) {
        println(s"${o.topic}  ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    })

    val words: DStream[String] = kafkaStream.flatMap(t => t.value().split(" "))

    //    val words: DStream[String] = socketStream.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))

    //无状态操作
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    //    ("------当前窗口数据--------")
    wordCounts.print

    //有状态操作，更新状态,将所有批次（即采集周期）的词频累计
    val updatedWordCounts: DStream[(String, Int)] = pairs.updateStateByKey {
      case (seq, buffer) => {
        val sum = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }
    //    ("------合计数据--------")
    updatedWordCounts.print



    // Start the computation
    // 通过 streamingContext.start()来启动消息采集和处理
    scc.start()

    // Wait for the computation to terminate
    // 通过streamingContext.stop()来手动终止处理程序
    scc.awaitTermination()

  }
}
