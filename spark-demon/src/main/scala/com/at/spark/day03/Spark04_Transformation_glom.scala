package com.at.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-14 1:47
 */
object Spark04_Transformation_glom {

  /*

     glom
       将RDD一个分区中的元素，组合成一个新的数组

    */


  def main(args: Array[String]): Unit = {



    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4), 2)

    println("------------ 转换前 --------------")
    rdd.mapPartitionsWithIndex((index,datas) => {
      println(index + " ----> " + datas.mkString(","))
      datas
    }).collect()

    val res: RDD[Array[Int]] = rdd.glom()
    println("------------ 转换后 --------------")
    res.mapPartitionsWithIndex((index,datas) => {
      println(index + " ----> " + datas.next().mkString(","))
      datas
    }).collect()


    // 关闭连接
    sc.stop()

  }

}
