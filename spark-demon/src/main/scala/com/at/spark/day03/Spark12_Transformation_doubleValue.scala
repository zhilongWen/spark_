package com.at.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 14:18
 */
object Spark12_Transformation_doubleValue {

  def main(args: Array[String]): Unit = {

    /*
     Desc: 转换算子-双value类型
    -合集
      union
    -交集
      intersect---->intersection
    -差集
      diff--------->subtract
    -拉链
      zip

     */

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val rdd2: RDD[Int] = sc.makeRDD(List(4,5,6,7),3)

    //合集
    //val newRDD: RDD[Int] = rdd1.union(rdd2)

    //交集
    //val newRDD: RDD[Int] = rdd1.intersection(rdd2)

    //差集
    //val newRDD: RDD[Int] = rdd1.subtract(rdd2)
    //val newRDD: RDD[Int] = rdd2.subtract(rdd1)

    //拉链
    //要求：分区数必须一致，分区中元素的个数必须一致
    //Can only zip RDDs with same number of elements in each partition
    //Can't zip RDDs with unequal numbers of partitions
    val newRDD: RDD[(Int, Int)] = rdd1.zip(rdd2)

    newRDD.collect().foreach(println)





    // 关闭连接
    sc.stop()


  }

}
