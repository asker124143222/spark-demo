package com.home.spark

import java.sql.{DriverManager, PreparedStatement}
import java.time.LocalDateTime

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @Author: xu.dm
  * @Date: 2019/12/5 15:21
  * @Version: 1.0
  * @Description: 数据写入mysql
  **/
object Ex_mysql2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark mysql demo")

    val sc = new SparkContext(conf)

    val driverClassName = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/busdata?characterEncoding=utf8&useSSL=false"
    val user = "root"
    val password = "root"


    //写入
    val logBuffer = mutable.ListBuffer[(String, String, String, String, String, String)]()
    import java.time.format.DateTimeFormatter
    val ofPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")
    for (i <- 1 to 100) {
      logBuffer.+=(("write" + i, "写入测试" + i, "localhost" + i, LocalDateTime.now().format(ofPattern), "spark", LocalDateTime.now().format(ofPattern)))
    }

    //    logBuffer.foreach(println)
    val logRDD: RDD[(String, String, String, String, String, String)] = sc.makeRDD(logBuffer)


    //为了减少连接创建次数，使用foreachPartition，而不是foreach
    //缺陷：所有按Partition方式传输整个迭代器的方式都有OOM的风险
    logRDD.foreachPartition(logData => {
      Class.forName(driverClassName)
      val connection = DriverManager.getConnection(url, user, password)
      val sql = "insert into syslog(action, event, host, insertTime, userName, update_Time) values(?,?,?,?,?,?)"
      val statement: PreparedStatement = connection.prepareStatement(sql)
      try {
        logData.foreach {
          case (action, event, host, insertTime, userName, updateTime) => {

            statement.setString(1, action)
            statement.setString(2, event)
            statement.setString(3, host)
            statement.setString(4, insertTime)
            statement.setString(5, userName)
            statement.setString(6, updateTime)
            statement.executeUpdate()
          }
        }
      }
      finally {
        if(statement!=null) statement.close()
        if(connection!=null) connection.close()
      }


      connection.close()
    }
    )


    sc.stop()
  }

}
