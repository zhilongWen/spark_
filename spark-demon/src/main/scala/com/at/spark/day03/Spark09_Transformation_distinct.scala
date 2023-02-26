package com.at.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 13:26
 */
object Spark09_Transformation_distinct {

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc .makeRDD(List(1, 2, 3, 4, 5, 5, 4, 3, 3), 5)

    rdd.mapPartitionsWithIndex{
      case (index,datas) => {
        println(index + " ----> " + datas.mkString(","))
      }
        datas
    }.collect()

    println(" ========================================== ")


    val res: RDD[Int] = rdd.distinct(3) //rdd.distinct()


    res.mapPartitionsWithIndex{
      case (index,datas) => {
        println(index + " ----> " + datas.mkString(","))
      }
        datas
    }.collect()


//    Thread.sleep(10000000)


    // 关闭连接
    sc.stop()



  }

}
