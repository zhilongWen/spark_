package com.at.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON


/**
 * @author zero
 * @create 2021-03-16 23:31
 */
object Spark03_readJson {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\input\\test.json")

    val res: RDD[Option[Any]] = rdd.map(JSON.parseFull)

    res.collect().foreach(println)



    // 关闭连接
    sc.stop()



  }
}
