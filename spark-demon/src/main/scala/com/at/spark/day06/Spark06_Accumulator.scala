package com.at.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-17 0:39
 */
object Spark06_Accumulator {

  /*
  累加器：分布式共享只写变量。（Task和Task之间不能读数据）
   */

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    //创建一个RDD，    单值RDD，直接并对数据进行求和，不存在shuffle过程

//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    println(rdd.sum())
//    println(rdd.reduce(_ + _))

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)),2)

    /*
    //存在shuffle，效率较低
    val value: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
    value.map(_._2).collect().foreach(println)
    */

    //如果定义一个普通的变量，那么在Driver定义，Excutor会创建变量的副本，算子都是对副本进行操作，Driver端的变量不会更新
   /*
   var sum: Int = 0
    rdd.foreach{
      case (word,count) => {
        sum+=count
        println("---->" + sum)
      }
    }
    println(sum) //0
    */

    //如果需要通过Excutor，对Driver端定义的变量进行更新，需要定义为累加器
    //累加器和普通的变量相比，会将Excutor端的结果，收集到Driver端进行汇总

    val sum: LongAccumulator = sc.longAccumulator("myAcc")
    rdd.foreach{
      case (word,count) => {
        sum.add(count)
        println("---->" + sum)
      }
    }
    println(sum.value)


    // 关闭连接
    sc.stop()



  }

}


