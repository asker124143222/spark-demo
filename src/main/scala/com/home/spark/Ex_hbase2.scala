package com.home.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @Author: xu.dm
  * @Date: 2019/12/9 15:30
  * @Version: 1.0
  * @Description: 写入hbase
  **/
object Ex_hbase2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark hbase write")
    val sc = new SparkContext(conf)


    val dataList = mutable.ListBuffer[(String, String, Int, String)]()

    for (i <- 1 to 10) {
      dataList.+=(("rr" + i, "qq" + i, 10 + i, "vv" + i))
    }
    //    val tuples = List(("r4","q4",4,"v4"),("r5","q5",4,"v5"))
    val dataRDD: RDD[(String, String, Int, String)] = sc.makeRDD(dataList)

    val putRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
      case (rowKey, qualifier, timestamp, value) => {
        val key: Array[Byte] = Bytes.toBytes(rowKey)
        val put = new Put(key)
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(qualifier), timestamp, Bytes.toBytes(value))
        (new ImmutableBytesWritable(key), put)
      }
    }

    val configuration: Configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", "192.168.44.10")

    val jobConf = new JobConf(configuration)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "demoTable")

    putRDD.saveAsHadoopDataset(jobConf)

    sc.stop()
  }
}
