package com.at.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 21:25
 */
object Spark08_Transformation_mapValues {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "d"), (2, "b"), (3, "c")))

    val newRDD: RDD[(Int, String)] = rdd.mapValues("|||" + _)

    newRDD.collect().foreach(println)


    // 关闭连接
    sc.stop()
  }

}
