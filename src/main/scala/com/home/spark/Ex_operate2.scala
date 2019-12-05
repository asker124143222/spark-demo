package com.home.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Ex_operate2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[*]").setAppName("Spark Demo")

    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 0, 222, 2, 443, 2, 3, 4, 5, 5, 6, 10, 1, 2, 35555, 998), 3)

    //todo glom 将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    glomRDD.foreach(x => {
      println("max:" + x.max + " --> " + x.mkString(","))
    })


    //todo groupby
    val groupRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(_ % 2)
    groupRDD.collect().foreach(println)


    //todo sample
    // 以指定的随机种子随机抽样出数量为fraction的数据，
    // withReplacement表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样，seed用于指定随机数生成器种子
    // 数据抽样可以用来处理数据倾斜
    val sampleRDD: RDD[Int] = listRDD.sample(false, 0.4, 1)
    println("sample: " + sampleRDD.collect().mkString(","))

    //withReplacement=true的采样可能会有重复数据，因为每次样本都被放回。
    val sampleRDD2: RDD[Int] = listRDD.sample(true, 0.4, 1)
    println("sample: " + sampleRDD2.collect().mkString(","))


    //todo distinct
    // distinct操作会导致shuffle，shuffle是一个比较消耗资源和时间的操作
    val distinctRDD: RDD[Int] = listRDD.distinct()
    println("distinct: " + distinctRDD.collect().mkString(","))
    //    distinctRDD.saveAsTextFile("output")

    //todo coalesce
    // 缩减分区数，用于大数据集过滤后，提高小数据集的执行效率
    val seqRDD: RDD[Int] = sc.parallelize(1 to 16, 4)
    val coalesceRDD: RDD[Int] = seqRDD.coalesce(3)
    println("coalesce : " + coalesceRDD.getNumPartitions)
    println("coalesec2 : " + coalesceRDD.partitions.size)

    val coalesce3: RDD[Int] = seqRDD.coalesce(2, true)
    println("coalesce3 : "+coalesce3.collect().mkString(","))
    coalesce3.glom().collect().foreach(x => {
      println("coalesce3 and glom : "+x.mkString(","))
    })

    //底层调用seqRDD.coalesce(2, true)
    //val repartitionRDD: RDD[Int] = seqRDD.repartition(2)

    sc.stop()
  }
}
