package com.home.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @Author: xu.dm
  * @Date: 2019/12/19 15:07
  * @Version: 1.0
  * @Description: 自定义用户函数udf
  **/
object Ex_sparkUDAF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("spark udf").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()


    //自定义聚合函数
    //创建聚合函数对象
    val myUdaf = new MyAgeAvgFunc

    //注册自定义函数
    spark.udf.register("ageAvg",myUdaf)

    //使用聚合函数
    val frame: DataFrame = spark.read.json("input/userinfo.json")
    frame.createOrReplaceTempView("userinfo")
    spark.sql("select ageAvg(age) from userinfo").show()

    spark.stop()
  }
}

//声明自定义函数
//实现对年龄的平均，数据如：{ "name": "tom", "age" : 20}
class MyAgeAvgFunc extends UserDefinedAggregateFunction {
  //函数输入的数据结构，本例中只有年龄是输入数据
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //计算时的数据结构（缓冲区）
  // 本例中有要计算年龄平均值，必须有两个计算结构，一个是年龄总计（sum），一个是年龄个数（count）
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  //函数返回的数据类型
  override def dataType: DataType = DoubleType

  //函数是否稳定
  override def deterministic: Boolean = true

  //计算前缓冲区的初始化,结构类似数组，这里缓冲区与之前定义的bufferSchema顺序一致
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //sum
    buffer(0) = 0L
    //count
    buffer(1) = 0L
  }

  //根据查询结果更新缓冲区数据，input是每次进入的数据,其数据结构与之前定义的inputSchema相同
  //本例中每次输入的数据只有一个就是年龄
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(input.isNullAt(0)) return
    //sum
    buffer(0) = buffer.getLong(0) + input.getLong(0)

    //count，每次来一个数据加1
    buffer(1) = buffer.getLong(1) + 1
  }

  //将多个节点的缓冲区合并到一起（因为spark是分布式的）
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)

    //count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算最终结果，本例中就是(sum / count)
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
