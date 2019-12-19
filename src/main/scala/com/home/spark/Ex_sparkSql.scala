package com.home.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * @Author: xu.dm
  * @Date: 2019/12/12 15:41
  * @Version: 1.0
  * @Description: sparksql，通过构建SparkSession来实现
  *               DataFrame，是数据结构，类似数据库表，Dataframe的劣势在于在编译期缺少类型安全检查，导致运行时出错.
  *               DataSet，是数据集，比DataFrame更进一步，是对数据结构进行对象式的描述
  *
  *               todo
  *               1）是Dataframe API的一个扩展，是Spark最新的数据抽象
  *               2）用户友好的API风格，既具有类型安全检查也具有Dataframe的查询优化特性。
  *               3）Dataset支持编解码器，当需要访问非堆上的数据时可以避免反序列化整个对象，提高了效率。
  *               4）样例类被用来在Dataset中定义数据的结构信息，样例类中每个属性的名称直接映射到DataSet中的字段名称。
  *               5） Dataframe是Dataset的特列，DataFrame=Dataset[Row] ，所以可以通过as方法将Dataframe转换为Dataset。
  *               Row是一个类型，跟Car、Person这些的类型一样，所有的表结构信息我都用Row来表示。
  *               6）DataSet是强类型的。比如可以有Dataset[Car]，Dataset[Person].
  *               7）DataFrame只是知道字段，但是不知道字段的类型，所以在执行这些操作的时候是没办法在编译的时候检查是否类型失败的，
  *               比如你可以对一个String进行减法操作，在执行的时候才报错，而DataSet不仅仅知道字段，而且知道字段类型，
  *               所以有更严格的错误检查。就跟JSON对象和类对象之间的类比。
  **/
object Ex_sparkSql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark session")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("input/userinfo.json")

    // df.show()

    df.createOrReplaceTempView("userinfo")

    spark.sql("select * from userinfo where age=30").show()


    //通过sparkSession生成rdd
    val rdd: RDD[(String, String)] = spark.sparkContext.textFile("input/1.txt").map(line => {
      val s = line.split(" ")
      (s(0), s(1))
    })
//    rdd.collect().foreach(println)

    //rdd转换成df或者ds需要SparkSession实例的隐式转换
    //导入隐式转换，注意这里的spark不是包名，而是SparkSession的对象名
    import spark.implicits._

    //rdd转成DataFrame
    val frame: DataFrame = rdd.toDF("name","value")

    //DataFrame转成DataSet
    val ds: Dataset[MyClass] = frame.as[MyClass]

    //ds转成df
    val df2: DataFrame = ds.toDF()

    //df转成rdd
    val rdd2 : RDD[Row]= df2.rdd

    //打印
    rdd2.foreach(row=>{
      println(row.getString(0)+"  -- "+row.getString(1))
    })

    val myRDD: RDD[MyClass] = rdd.map {
      case (name, value) => {
        MyClass(name, value)
      }
    }
    val myDS = myRDD.toDS()

    println("---"*10)
    myDS.show()

    spark.stop()
  }
}

case class MyClass(name:String,value:String)