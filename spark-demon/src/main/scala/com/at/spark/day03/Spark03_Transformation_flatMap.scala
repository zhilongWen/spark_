package com.at.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-14 1:45
 */
object Spark03_Transformation_flatMap {
  def main(args: Array[String]): Unit = {


    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[List[Int]] = sc.parallelize(List(List(1, 2, 3), List(4, 5), List(6, 7, 9)))


    val res: RDD[Int] = rdd.flatMap(e => e)

    res.collect().foreach(println)

    // 关闭连接
    sc.stop()


  }

}
