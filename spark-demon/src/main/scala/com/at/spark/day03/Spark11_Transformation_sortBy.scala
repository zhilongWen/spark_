package com.at.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 13:53
 */
object Spark11_Transformation_sortBy {

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 4, 3, 2))


    //升序排序
    //val sortedRDD: RDD[Int] = numRDD.sortBy(num=>num)
    //降序排序
    //val sortedRDD: RDD[Int] = numRDD.sortBy(num=> -num)
    //val sortedRDD: RDD[Int] = numRDD.sortBy(num=> num,false)

    val strRDD: RDD[String] = sc.makeRDD(List("1","4","3","22"))

    //按照字符串字符字典顺序进行排序
    //val sortedRDD: RDD[String] = strRDD.sortBy(elem=>elem)

    //按照字符串转换为整数后的大小进行排序
    val sortedRDD: RDD[String] = strRDD.sortBy(_.toInt)

    sortedRDD.collect().foreach(println)


    // 关闭连接
    sc.stop()

  }

}
