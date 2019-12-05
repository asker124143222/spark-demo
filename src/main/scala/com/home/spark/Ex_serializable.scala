package com.home.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Ex_serializable {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[*]").setAppName("spark serializable demo")

    val sc = new SparkContext(conf)

    val rddArray = sc.parallelize(Array("hadoop","hive","hbase","java","scala","c#"))
    val search = new Search("h")
    val result: RDD[String] = search.getMatch(rddArray)
    result.collect().foreach(println)

    sc.stop()
  }
}


class Search(query: String) extends java.io.Serializable {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatch(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  def getMatch2(rdd: RDD[String]): RDD[String] = {
    //可以采用本地变量代替query
//    val q = query //q是String类变量，已经实现序列化
//    rdd.filter(x=>x.contains(q))
    //todo query是Search类的成员变量，所以使用query进行传输的时候需要对Search类对象进行序列化
    rdd.filter(x=>x.contains(query))
  }
}
