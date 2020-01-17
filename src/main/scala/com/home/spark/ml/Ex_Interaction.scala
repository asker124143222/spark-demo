package com.home.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{Interaction, VectorAssembler}
import org.apache.spark.sql.SparkSession

/**
  * @Author: xu.dm
  * @Date: 2020/1/16 11:22
  * @Version: 1.0
  * @Description: 笛卡尔特征交互
  * 实现特征交互转换。 此转换器接受Double和Vector类型的列，并输出其特征交互的展平向量。
  * 为了处理交互，我们首先对任何标称特征进行一次热编码。 然后，生成特征叉积的向量。
  * 例如，给定输入要素值Double（2）和Vector（3，4），如果所有输入要素都是数字，则输出将为Vector（6，8）。
  * 如果第一个特征标称是具有四个类别，则输出将是“ Vector（0，0，0，0，3，4，0，0）”。
  **/
object Ex_Interaction {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf(true).setMaster("local[2]").setAppName("spark ml")
    val spark = SparkSession.builder().config(conf).getOrCreate()


    val df = spark.createDataFrame(Seq(
      (1, 1, 2, 3, 8, 4, 5),
      (2, 4, 3, 8, 7, 9, 8),
      (3, 6, 1, 9, 2, 3, 6),
      (4, 10, 8, 6, 9, 4, 5),
      (5, 9, 2, 7, 10, 7, 3),
      (6, 1, 1, 4, 2, 8, 4)
    )).toDF("id1", "id2", "id3", "id4", "id5", "id6", "id7")

    val assembler1 = new VectorAssembler().setInputCols(Array("id2","id3","id4")).setOutputCol("vector1")
    val assembled1 = assembler1.transform(df)
    assembled1.show(false)

    val assembler2 = new VectorAssembler().setInputCols(Array("id5","id6","id7")).setOutputCol("vector2")
    val assembled2 = assembler2.transform(assembled1)
    assembled2.show(false)

    val interaction = new Interaction().setInputCols(Array("id1","vector1","vector2")).setOutputCol("interactedCol")
    val result = interaction.transform(assembled2)

    result.show(false)
    spark.stop()
  }

}
