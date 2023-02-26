package com.at.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-13 1:08
 *
 *
 */
object Spark01_CreateRDD {


  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Spark01_CreateRDD").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    //--------  通过读取内存集合中的数据，创建RDD  ---------------------------------

    /*
    val list: List[Int] = List(1, 2, 3, 4, 5)
//    val rdd: RDD[Int] =  sc.makeRDD(list) //sc.parallelize(list)
     */

    //--------  通过读取外部文件，创建RDD ---------------------------------

    //从本地文件中读取数据
//    val rdd: RDD[String] = sc.textFile("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\input\\1.txt")

    //从HDFS服务器上读取数据
    val rdd: RDD[String] = sc.textFile("hdfs://hadoop102:8020/input")



    rdd.foreach(println)

    // 关闭连接
    sc.stop()


    /*

      通过读取内存集合中的数据，创建RDD
        sc.parallelize(Seq)
        sc.makeRDD(Seq) makeRDD的底层实际还是调用parallelize


      通过读取外部文件，创建RDD
        -从本地文件中读取数据
        -从HDFS服务器上读取数据
          sc.textFile(本地文件路径或HDFS文件路径)

     */


  }

}
