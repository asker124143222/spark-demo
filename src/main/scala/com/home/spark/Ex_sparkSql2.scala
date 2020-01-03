package com.home.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author: xu.dm
  * @Date: 2020/1/2 13:53
  * @Version: 1.0
  * @Description: TODO
  **/
object Ex_sparkSql2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("spark sql").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

//    val driverClassName = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/busdata?characterEncoding=utf8&useSSL=false"
    val user = "root"
    val password = "root"

    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", "syslog")
      .load()
    df.printSchema()

    val countByUserName: DataFrame = df.groupBy("userName").count()
    countByUserName.show()

    countByUserName.write.format("json").save("output")

    spark.stop()

  }
}
