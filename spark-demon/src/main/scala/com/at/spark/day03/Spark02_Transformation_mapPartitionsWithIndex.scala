package com.at.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-14 1:36
 */
object Spark02_Transformation_mapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3)

//    val res: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, datas) => {
//      datas.map((index, _))
//    })


    //第二个分区数据*2，其余分区数据保持不变
    val res: RDD[Int] = rdd.mapPartitionsWithIndex((index, datas) => {
      index match {
        case 1 => datas.map(_ * 2)
        case _ => datas
      }
    })

    res.collect().foreach(println)



    // 关闭连接
    sc.stop()



  }

}
