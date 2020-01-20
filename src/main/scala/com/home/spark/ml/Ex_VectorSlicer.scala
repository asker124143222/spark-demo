package com.home.spark.ml

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @Author: xu.dm
  * @Date: 2020/1/18 14:26
  * @Version: 1.0
  * @Description: 向量切片
  *  此类采用特征向量，并输出带有原始特征子数组的新特征向量。
  *
  *  可以使用索引（setIndices（））或名称（setNames（））来指定功能的子集。 必须至少选择一项功能。 不允许使用重复功能，因此所选索引和名称之间不能有重叠。
  *
  *  输出矢量将首先对具有选定索引的要素（以给定的顺序）进行排序，然后是选定名称（以给定的顺序）进行排序。
  **/
object Ex_VectorSlicer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val data = util.Arrays.asList(
      Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3)))),
      Row(Vectors.dense(-2.0, 2.3, 0.0))
    )

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val dataset = spark.createDataFrame(data, StructType(Array(attrGroup.toStructField())))

    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    slicer.setIndices(Array(1)).setNames(Array("f3"))
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))

    val output = slicer.transform(dataset)
    output.show(false)

    val dataset2 = spark.createDataFrame(
      Seq(
        (0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5,99.0,100), 1.0),
        (0, 18, 1.0, Vectors.dense(0.0, 11.0,15.0,22.0,101), 0.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    val slicer2 = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")
    slicer2.setIndices(Array(1,3,4))

    val result = slicer2.transform(dataset2)
    result.show(false)

    spark.stop()
  }
}
