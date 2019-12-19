package com.home.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
  * @Author: xu.dm
  * @Date: 2019/12/19 16:44
  * @Version: 1.0
  * @Description: 使用强类型实现用户自定义函数
  **/
object Ex_sparkUDAF2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("spark udf class").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //rdd转换成df或者ds需要SparkSession实例的隐式转换
    //导入隐式转换，注意这里的spark不是包名，而是SparkSession的对象名
    import spark.implicits._

    //创建聚合函数对象
    val myAvgFunc = new MyAgeAvgClassFunc
    val avgCol: TypedColumn[UserBean, Double] = myAvgFunc.toColumn.name("avgAge")
    val frame = spark.read.json("input/userinfo.json")
    val userDS: Dataset[UserBean] = frame.as[UserBean]
    //应用函数
    userDS.select(avgCol).show()

    spark.stop()
  }
}


case class UserBean(name: String, age: BigInt)

case class AvgBuffer(var sum: BigInt, var count: Int)

//声明用户自定义函数（强类型方式）
//继承Aggregator，设定泛型
//实现方法
class MyAgeAvgClassFunc extends Aggregator[UserBean, AvgBuffer, Double] {
  //初始化缓冲区
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  //聚合数据
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    if(a.age == null) return b
    b.sum = b.sum + a.age
    b.count = b.count + 1

    b
  }

  //缓冲区合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count

    b1
  }

  //完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}