package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/17 15:03
  * @Version: 1.0
  * @Description: 分位数离散器
  * QuantileDiscretizer 按分位数，对给出的数据列进行离散化分箱处理。
  * 箱数由numBuckets参数设置。
  * 例如，如果输入的不同值太少而无法创建足够的不同分位数，则所使用的存储桶的数量可能会小于该值。
  *
  * NaN值：在QuantileDiscretizer拟合过程中，将从柱中除去NaN值。这将产生一个Bucketizer模型进行预测。
  * 在转换期间，Bucketizer在数据集中找到NaN值时将引发错误，但是用户也可以通过设置handleInvalid选择保留还是删除数据集中的NaN值。
  * 如果用户选择保留NaN值，则将对其进行特殊处理并将其放入自己的存储桶中，
  * 例如，如果使用4个存储桶，则将非NaN数据放入存储桶[0-3]中，但NaN将被存储放在一个特殊的桶中[4]。
  *
  * 算法：分箱范围是使用近似算法选择的（有关详细说明，请参见aboutQuantile的文档）。
  * 可以使用relativeError参数控制近似精度。设置为零时，将计算精确的分位数（注意：计算精确的分位数是一项昂贵的操作）。
  * 分箱的上下边界将是-Infinity和+ Infinity，覆盖所有实数值。
  *
  **/
object Ex_QuantileDiscretizer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    val df = spark.createDataFrame(data).toDF("id", "hour")

    val discretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("result")
      .setNumBuckets(3)

    val result = discretizer.fit(df).transform(df)
    result.show(false)

    spark.stop()
  }
}
