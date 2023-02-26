package com.at.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 13:41
 */
object Spark10_Transformation_coalesce {

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    rdd.mapPartitionsWithIndex {
      case (index, datas) => {
        println(index + " -----> " + datas.mkString(","))
        datas
      }
    }.collect()

    println("=========================================================")

    //    val res: RDD[Int] = rdd.coalesce(2)
    //注意：默认情况下，如果使用coalesce扩大分区是不起作用的  。因为底层没有执行shuffle
//    val res: RDD[Int] = rdd.coalesce(4)

//    val res: RDD[Int] = rdd.repartition(4)
    val res: RDD[Int] = rdd.coalesce(4, true)
    /*
      repartition 实际上是调用的 coalesce 并且默认开启shuffle
        coalesce(numPartitions, shuffle = true)
     */

    res.mapPartitionsWithIndex {
      case (index, datas) => {
        println(index + " -----> " + datas.mkString(","))
        datas
      }
    }.collect()




    // 关闭连接
    sc.stop()


  }

}
