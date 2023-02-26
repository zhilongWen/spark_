package com.at.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 17:56
 */
object Spark01_Transformation_partitionBy {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //    rdd.partitionBy

    //RDD本身是没有partitionBy这个算子的，通过隐式转换动态给kv类型的RDD扩展的功能
    // 隐式转换 implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)

    rdd.mapPartitionsWithIndex{
      (index,datas) => {
        println(index + " ====> " + datas.mkString(","))
        datas
      }
    }.collect()

    println("-----------------------------------------------")

//    val res: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))
    val res: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))

    res.mapPartitionsWithIndex{
      (index,datas) => {
        println(index + " ====> " + datas.mkString(","))
        datas
      }
    }.collect()



    // 关闭连接
    sc.stop()

  }

}
//自定义 partitioner
class MyPartitioner(partitions: Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    // 分区规则
    1
  }
}
