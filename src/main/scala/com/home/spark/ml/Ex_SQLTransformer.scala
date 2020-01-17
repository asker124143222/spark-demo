package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/17 16:51
  * @Version: 1.0
  * @Description:
  * SQLTransformer实现由SQL语句定义的转换。
  * 但前仅支持SQL语法，例如“ SELECT ... FROM __THIS__ ...”，其中“ __THIS__”代表输入数据集的基础表。
  * select子句指定要在输出中显示的字段，常量和表达式，并且可以是Spark SQL支持的任何select子句。
  * 用户还可以使用Spark SQL内置函数和UDF对这些选定的列进行操作。
  * 支持的语法如下：
    SELECT a, a + b AS a_b FROM __THIS__
    SELECT a, SQRT(b) AS b_sqrt FROM __THIS__ where a > 5
    SELECT a, b, SUM(c) AS c_sum FROM __THIS__ GROUP BY a, b

  **/
object Ex_SQLTransformer {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.createDataFrame(
      Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")

    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")

    sqlTrans.transform(df).show()

    spark.stop()
  }
}
