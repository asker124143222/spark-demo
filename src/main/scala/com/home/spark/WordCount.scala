package com.home.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    var filePath : String ="input"
    if(args.length  > 0)
      filePath = args(0)
    //获取环境
    val conf: SparkConf = new SparkConf(true).setAppName("SparkWordCount").setMaster("local[*]")

    //获取上下文
    val sc: SparkContext = new SparkContext(conf)

    //读取每一行

    val lines: RDD[String] = sc.textFile(filePath)

    //扁平化，将每行数据拆分成单个词（自定义业务逻辑）
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //结构转换，对每个词获得初始词频
    val wordToOne: RDD[(String, Int)] = words.map((_,1))

    //词频计数
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
    //按词频数量降序排序
    val wordToSorted: RDD[(String, Int)] = wordToSum.sortBy(_._2,false)

    //数据输出
    val result: Array[(String, Int)] = wordToSorted.collect()

    //打印
    result.foreach(println)

    //关闭上下文
    sc.stop()
  }
}
