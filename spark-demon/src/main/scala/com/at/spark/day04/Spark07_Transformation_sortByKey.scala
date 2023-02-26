package com.at.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 20:45
 */
object Spark07_Transformation_sortByKey {

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
/*

    //默认升序
    rdd.sortByKey().collect().foreach(println)
    rdd.sortByKey(false).collect().foreach(println)
*/
    val stdList: List[(Student, Int)] = List(
      (new Student("jingjing", 18), 1),
      (new Student("bangzhang", 18), 1),
      (new Student("jingjing", 19), 1),
      (new Student("luoxiang", 18), 1),
      (new Student("jingjing", 20), 1)
    )
    val stdRDD: RDD[(Student, Int)] = sc.makeRDD(stdList)
    val res: RDD[(Student, Int)] = stdRDD.sortByKey()
    res.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }

}

class Student(var name:String,var age:Int) extends Ordered[Student] with Serializable {

  //指定比较规则
  override def compare(that: Student): Int = {

    //升序
    var res: Int = this.name.compareTo(that.name)

    if(res == 0){
      //降序
      res = that.age - this.age
    }

    res

  }

  override def toString: String = s"Student($name, $age)"

}