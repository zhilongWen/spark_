package com.at.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-14 2:01
 */
object Spark06_WordCount {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /*
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"))

    rdd.flatMap(_.split(" ")).groupBy(elem => elem).map(datas => (datas._1,datas._2.size)).collect().foreach(println)
     */



    val tupleList = List(("Hello Scala Spark World ", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

 /*
    val list: List[Array[(String, Int)]] = tupleList.map(kv => {
      val strings: Array[String] = kv._1.split(" ")
      strings.map((_, kv._2))
    })

    list.flatMap(ele => ele).groupBy(_._1).map{
      case (word,datas) => {
      }
    }.foreach(println)
        (word,datas.map(_._2).sum)
//简化
    tupleList.flatMap(kv => {
      kv._1.split(" ").map(e => (e,kv._2))
    }).groupBy(kv => kv._1).map{case (w,kv) => (w,kv.map(_._2).sum)}.foreach(println)
*/



    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Hello Scala", 2), ("Hello Spark", 3), ("Hello World", 2)))

    val flatMapRDD: RDD[(String, Int)] = rdd.flatMap {
      case (word, count) => {
        word.split(" ").map(w => (w, count))
      }
    }

    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = flatMapRDD.groupBy(kv => kv._1)

    val res: RDD[(String, Int)] = groupByRDD.map {
      case (word, datas) => {
        (word, datas.map(_._2).sum)
      }
    }

    res.foreach(println)


    // 关闭连接
    sc.stop()
  }

}
