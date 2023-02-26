package com.at.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-12 21:51
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    //创建SparkContext配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建SparkContext对象
    val sc: SparkContext = new SparkContext(conf)


    //读取外部文件
    val textRDD: RDD[String] = sc.textFile("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\input")

//    textRDD.mapPartitions()
//    textRDD.map()

    //对读取到的内容进行切割并进行扁平化操作
    val flatMapRDD: RDD[String] = textRDD.flatMap(_.split(" "))

    //对数据集中的内容进行结构的转换 ---计数
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))

    //对相同的单词  出现的次数进行汇总
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    //将执行的结构进行收集
    val res: Array[(String, Int)] = reduceRDD.collect()

    res.foreach(println)

    //    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))

    sc.stop()


  }

}
