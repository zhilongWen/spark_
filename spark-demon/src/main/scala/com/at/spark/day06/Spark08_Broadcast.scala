package com.at.spark.day06

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-17 1:27
 */
object Spark08_Broadcast {
  /*
    -广播变量
      分布式共享只读变量
   */

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //想实现类似join效果   (a,(1,4)),(b,(2,5)),(c,(3,6))
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val list: List[(String, Int)] = List(("a",4),("b",5),("c",6))

    //创建一个广播变量
    val broadcastList: Broadcast[List[(String, Int)]] = sc.broadcast(list)

    val resRDD: RDD[(String, (Int, Int))] = rdd.map {
      case (k1, v1) => {
        var v3 = 0
        //for ((k2, v2) <- list) {
        for ((k2, v2) <- broadcastList.value) {
          if (k1 == k2) {
            v3 = v2
          }
        }
        (k1, (v1, v3))
      }
    }
    resRDD.collect().foreach(println)


    // 关闭连接
    sc.stop()
  }

}
