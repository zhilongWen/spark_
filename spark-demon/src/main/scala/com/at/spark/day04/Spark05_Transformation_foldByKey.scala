package com.at.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 19:52
 */
object Spark05_Transformation_foldByKey {

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)


    rdd.reduceByKey(_+_).collect().foreach(println)
    rdd.groupByKey().map{case (index,datas) => (index,datas.sum)}.collect().foreach(println)
    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)

    //foldByKey是AggregateByKey简化版本
    rdd.foldByKey(0)(_+_).collect().foreach(println)

    /*
      如果分区内和分区间计算规则一样，并且不需要指定初始值，那么优先使用reduceByKey

      如果分区内和分区间计算规则一样，并且需要指定初始值，那么优先使用foldByKey

      如果分区内和分区间计算规则不一样，并且需要指定初始值，那么优先使用aggregateByKeyByKey

     */




    // 关闭连接
    sc.stop()



  }

}
