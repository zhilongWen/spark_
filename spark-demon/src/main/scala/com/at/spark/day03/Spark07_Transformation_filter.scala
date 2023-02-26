package com.at.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 13:15
 */
object Spark07_Transformation_filter {

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("wangqiao","xiaojing","hanqi","chengjiang","xiaohao"))

    rdd.filter(_.contains("xiao")).collect().foreach(println)





    // 关闭连接
    sc.stop()


  }

}
