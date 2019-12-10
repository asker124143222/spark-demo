package com.home.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: xu.dm
  * @Date: 2019/12/6 16:09
  * @Version: 1.0
  * @Description: 从hbase读取数据
  **/
object Ex_hbase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setAppName("Spark hbase demo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val configuration: Configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", "192.168.44.10")
    configuration.set(TableInputFormat.INPUT_TABLE, "testtable")

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      configuration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )


//    hbaseRDD.foreach {
//      case (_, result) => {
//        //获取行键
//        val key = Bytes.toString(result.getRow)
//        //通过列族和列名获取列
//        val name = Bytes.toString(result.getValue("info".getBytes, "username".getBytes))
//        val age = Bytes.toString(result.getValue("ex".getBytes, "addr".getBytes))
//        println("Row key:" + key + "\tcf1.username:" + name + "\tcf1.addr:" + age)
//      }
//    }

    hbaseRDD.foreach {
      case (_, result) => {
        val cells = result.rawCells()
        val key = Bytes.toString(result.getRow)
        for (cell <- cells) {
          val columnFamily = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
          val columnName = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
          val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          printf("[key:%s]\t[family:%s] [column:%s] [value:%s]\n", key, columnFamily, columnName, value)
        }
      }
    }


    sc.stop()
  }

}
