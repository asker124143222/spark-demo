package com.home.spark


import java.sql.DriverManager
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: xu.dm
  * @Date: 2019/12/5 15:21
  * @Version: 1.0
  * @Description: 读取mysql
  **/
object Ex_mysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark mysql demo")

    val sc = new SparkContext(conf)

    val driverClassName = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/busdata?characterEncoding=utf8&useSSL=false"
    val user = "root"
    val password = "root"

    val sql = "select userId,userName,name from user where createTime > from_unixtime(?) and createTime < from_unixtime(?)"


    val connection = () => {
      Class.forName(driverClassName)
      DriverManager.getConnection(url, user, password)
    }

    val startTime = LocalDateTime.of(2018, 11, 3, 0, 0, 0)
    val endTime = LocalDateTime.of(2018, 11, 4, 0, 0)
    val startTimeStamp = startTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli / 1000
    val endTimeStamp = endTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli / 1000

    println("startTime: " + startTime + ", endTime: " + endTime)
    println("startTime: " + startTimeStamp + ", endTime: " + endTimeStamp)

    //读取
    val result: JdbcRDD[(Int, String, String)] = new JdbcRDD[(Int, String, String)](
      sc,
      connection,
      sql,
      startTimeStamp,
      endTimeStamp,
      2,
      rs => {

        val userId = rs.getInt(1)
        val userName = rs.getString(2)
        val name = rs.getString(3)
        //        println(s"id:${userId},userName:${userName},name:${name}")
        (userId, userName, name)
      }
    )
    result.collect().foreach(println)

    sc.stop()
  }

}
