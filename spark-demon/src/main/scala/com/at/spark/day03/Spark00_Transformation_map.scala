package com.at.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-14 1:23
 */
object Spark00_Transformation_map {

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)

    println("原分区数："+ rdd.partitions.size)

    val res: RDD[Int] = rdd.map(_ * 2)
    println("变换后区数："+ res.partitions.size)

    /*
        dependencies.head.rdd.asInstanceOf[RDD[U]].partitioner
         变换后分区数不变

     */


    res.foreach(println)

    // 关闭连接
    sc.stop()

  }

}
