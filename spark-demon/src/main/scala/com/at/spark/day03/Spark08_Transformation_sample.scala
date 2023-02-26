package com.at.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 13:16
 */
object Spark08_Transformation_sample {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /*
      -withReplacement  是否抽样放回
        true    抽样放回
        false   抽样不放回
      -fraction
        withReplacement=true   表示期望每一个元素出现的次数  >0
        withReplacement=false  表示RDD中每一个元素出现的概率[0,1]
      -seed 抽样算法初始值
        一般不需要指定
    */


    val rdd: RDD[Int] = sc.makeRDD(1 to 10)

    //抽样放回
//    val res: RDD[Int] = rdd.sample(true, 1)

    //抽样不放回
//    val res: RDD[Int] = rdd.sample(false, 0.5)
//
//    res.collect().foreach(println)


    val res: Array[Int] = rdd.takeSample(false, 2)

    res.foreach(println)


    // 关闭连接
    sc.stop()

  }

}
