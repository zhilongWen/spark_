package com.at.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-16 21:48
 */
object Spark02_checkpoint {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //设置检查点目录
    sc.setCheckpointDir("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\cp")

    //开发环境，应该将检查点目录设置在hdfs上
    // 设置访问HDFS集群的用户名
    //System.setProperty("HADOOP_USER_NAME","zero")
    //sc.setCheckpointDir("hdfs://hadoop202:8020/cp")


    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello bangzhang","hello jingjing"),2)

    //扁平映射
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))

    //结构转换
    val mapRDD: RDD[(String, Long)] = flatMapRDD.map {
      word => {
        (word, System.currentTimeMillis())
      }
    }


    //在开发环境，一般检查点和缓存配合使用
    mapRDD.cache()

    //设置检查点
    mapRDD.checkpoint()

    println(mapRDD.toDebugString)

    mapRDD.collect().foreach(println)

    println(" ========================================================== ")

    //释放缓存
    mapRDD.unpersist()

    println(mapRDD.toDebugString)
    mapRDD.collect().foreach(println)


    // 关闭连接
    sc.stop()
  }

  /*
  -cache
		底层调用的就是persist，默认存储在内存中

	-persist
		可以通过参数指定存储级别

	-checkpoint
		*可以当做缓存理解，存储在HDFS上，更稳定
		*为了避免容错执行时间过长

	-缓存不会切断血缘，但是检查点会切断血缘
   */

}
