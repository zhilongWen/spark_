package com.at.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-14 1:54
 */
object Spark05_Transformation_groupBy {

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9),2)

    println("---------- 分组前 ----------")
    rdd.mapPartitionsWithIndex((index,datas)=>{
      println(index + " -----> " + datas.mkString(","))
      datas
    }).collect()


    val res: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 3)

    println("---------- 分组后 ----------")
    res.mapPartitionsWithIndex((index,datas) => {
      println(index + " -----> " + datas.mkString(","))
      datas
    }).collect()

    // 关闭连接
    sc.stop()

    /*
      区比组范围大
      分区与分组没有关系
     */

  }

}
