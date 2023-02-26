package com.at.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

import scala.collection.mutable

/**
 * @author zero
 * @create 2021-03-17 0:55
 */
object Spark07_Accumulator {

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "HaHa", "Hello", "HaHa", "Spark", "Spark"))


    val acc = new MyAccumulator()
    //注册累加器
    sc.register(acc)

    //使用累加器
    rdd.foreach{
      word=>{
        acc.add(word)
      }
    }

    //输出累加器结果
    println(acc.value)


    // 关闭连接
    sc.stop()

  }

}

//定义一个类，继承AccumulatorV2
//泛型累加器输入和输出数据的类型
class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{
  //定义一个集合，集合单词以及出现次数
  private var map: mutable.Map[String, Int] = mutable.Map[String, Int]()
  //是否为初始状态
  override def isZero: Boolean = map.isEmpty
  //拷贝
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    val newAcc = new MyAccumulator
    newAcc.map = this.map
    newAcc
  }
  //重置
  override def reset(): Unit = map.clear()
  //向累加器中添加元素
  override def add(v: String): Unit = {

    if(v.startsWith("H")){
      //向可变集合中添加或者更新元素
      map(v) = map.getOrElse(v,0) +1
    }

  }
  //合并
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {

    var map1 = map
    var map2 = other.value

    //def foldLeft[B](z: B)(op: (B, A) => B): B = {
    map = map1.foldLeft(map2)((mn,kv) => {

      val k: String = kv._1
      val v: Int = kv._2

      mn(k) = mn.getOrElse(k,0) + v

      mn

    })



  }
  //获取累加器的值
  override def value: mutable.Map[String, Int] = map
}


