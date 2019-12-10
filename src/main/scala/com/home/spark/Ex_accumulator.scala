package com.home.spark

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @Author: xu.dm
  * @Date: 2019/12/10 10:33
  * @Version: 1.0
  * @Description: 累加器
  **/
object Ex_accumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("spark accumulator").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val list = mutable.ListBuffer[Int]()
    for (i <- 1 to 100) {
      list.+=(i)
    }

    val dataRDD: RDD[Int] = sc.makeRDD(list,4)


    //使用累加器来共享变量
    val accumulator: LongAccumulator = sc.longAccumulator

    dataRDD.foreach{
      case i:Int => {
        accumulator.add(i)
      }
    }

    println(s"sum : ${accumulator.value}")

    val fileRDD: RDD[String] = sc.textFile("input")
    val count = sc.longAccumulator
    val logAccumulator = new LogAccumulator()
    //注册自定义累加器
    sc.register(logAccumulator,"logAccumulator")
    fileRDD.foreach(s=>{
      if(s.contains("hello")){
        count.add(1)
        logAccumulator.add(s)
      }
    })
    println(s"input 目录包含hello字符串的行 : ${count.value}")
    println(s"已经记录的日志：${logAccumulator.value}")


    sc.stop()
  }
}


//自定义累加器
class LogAccumulator extends AccumulatorV2[String, java.util.Set[String]]{

  private val logArrays: util.Set[String] with Object = new util.HashSet[String]()

  override def isZero: Boolean = {
    logArrays.isEmpty
  }

  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val newLogAccumulator = new LogAccumulator()
    logArrays.synchronized{
      newLogAccumulator.logArrays.addAll(logArrays)
    }
    newLogAccumulator
  }

  override def reset(): Unit = {
    logArrays.clear()
  }

  override def add(v: String): Unit = {
    logArrays.add(v)
  }

  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    other match {
      case o : LogAccumulator => logArrays.addAll(o.value)
    }
  }

  override def value: util.Set[String] = {
    java.util.Collections.unmodifiableSet(logArrays)
  }
}