package com.at.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 18:20
 */
object Spark03_Transformation_groupByKey {

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)),3)

    //(a,CompactBuffer((a,1), (a,5)))
//    rdd.groupBy(_._1).collect().foreach(println)

    //底层调用的是 PairRDDFunctions$combineByKeyWithClassTag 方法
    //(a,CompactBuffer(1, 5))
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey(2)
//    groupRDD.collect().foreach(println)

    val res: RDD[(String, Int)] = groupRDD.map {
      case (key, datas) => (key, datas.sum)
    }

    res.collect().foreach(println)


    // 关闭连接
    sc.stop()


  }
}
