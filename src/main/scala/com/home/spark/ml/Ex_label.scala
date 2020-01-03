package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature

/**
  * @Author: xu.dm
  * @Date: 2020/1/2 17:01
  * @Version: 1.0
  * @Description: 机器学习，训练样本数据，给生产数据打标签
  **/
object Ex_label {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark ml label")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val error_count = spark.sparkContext.longAccumulator("error_count")

    //载入训练数据，数据手工训练，给带有hello的数据打上1.0的标签，给没有hello的数据打上0.0
    val lineRDD: RDD[String] = spark.sparkContext.textFile("input/sample.txt")

    //rdd转换成df或者ds需要SparkSession实例的隐式转换
    //导入隐式转换，注意这里的spark不是包名，而是SparkSession的对象名
    import spark.implicits._


    //生成样本数据
    val df: DataFrame = lineRDD.map(line=>{
      val strings: Array[String] = line.split(",")
      if(strings.length==3){
        (strings(0),strings(1),strings(2))
      }
      else{
        error_count.add(1)
        ("-1",strings.mkString(" "),"0.0")
      }

    }).filter(s=> !s._1.equals("-1"))
      .toDF("id","text","label")

//    new Tokenizer()

    df.printSchema()
    df.show()


    println(s"错误数据计数 : ${error_count.value}")
    spark.stop()
  }
}
