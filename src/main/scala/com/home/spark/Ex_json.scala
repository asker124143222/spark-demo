package com.home.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object Ex_json {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark json demo")

    val sc = new SparkContext(conf)

    //按文本方式读取json，目标文件里每行是一个json对象
    val jsonRDD: RDD[String] = sc.textFile("input/userInfo.json")

    val result: RDD[Option[Any]] = jsonRDD.map(JSON.parseFull)

    result.map(x => {
      var temp = ""
      x.getOrElse() match {
        case a: Map[Any, Any] => {
          if (a.contains("sex"))
            temp += a.getOrElse("sex", "") + " "
          if (a.contains("age"))
            temp += a.getOrElse("age", 0)
          temp
        }


        case _ =>
      }
    }).collect().foreach(println)

    println("--" * 10)

    result.map(x => {
      val map = x.getOrElse()
      map match {
        case b:Map[String,_] => {
          b
        }
        case _ =>
      }
    }).collect().foreach(println)

    //    val result2: RDD[Any] = result.map(_.getOrElse())
    //    result2.foreach(println)


    sc.stop()
  }

}
