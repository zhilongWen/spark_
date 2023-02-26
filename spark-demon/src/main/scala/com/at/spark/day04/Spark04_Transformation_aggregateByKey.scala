package com.at.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 18:27
 */
object Spark04_Transformation_aggregateByKey {

  def main(args: Array[String]): Unit = {


    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

//    rdd.reduceByKey(_+_).collect().foreach(println)
//    rdd.groupByKey().map(kv => (kv._1,kv._2.sum)).collect().foreach(println)
//    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)


    //分区最大值，求和
    /*
        aggregateByKey(zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,combOp: (U, U) => U)
            zeroValue 初始值
            seqOp 分区内
            combOp 分区外
     */
    rdd.aggregateByKey(0)(math.max(_,_),_+_).collect().foreach(println)


    // 关闭连接
    sc.stop()


  }

}
