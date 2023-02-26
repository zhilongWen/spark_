package com.at.spark.day01

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author zero
 * @create 2021-06-24 11:05
 */
object t {


  def main(args:Array[String])= {

    val sc = new SparkContext()

    val res: RDD[(String, Int)] = sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)




  }

    
  
}
