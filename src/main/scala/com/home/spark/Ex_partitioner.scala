package com.home.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Ex_partitioner {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf(true).setAppName("spark partitioner").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rddList: RDD[(Int, String)] = sc.makeRDD(List((-100, "aa"), (0, "err"), (1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f")))

    val rddList2 = rddList.partitionBy(new MyPartitioner(4))

    rddList2.saveAsTextFile("output")


    sc.stop()

  }

}


class MyPartitioner(partitions: Int) extends Partitioner {

  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    //使用模式匹配
    key match {
      case i: Int =>
        println("case : " + i)
        if (i > 1) {
          partitions - 1
        } else {
          0
        }

      case _ => 0
    }
    //    if(key.isInstanceOf[Int]){
    //      if( key.asInstanceOf[Int] > 1){
    //        partitions-1
    //      }else{
    //        0
    //      }
    //    }else{
    //      0
    //    }
  }
}
