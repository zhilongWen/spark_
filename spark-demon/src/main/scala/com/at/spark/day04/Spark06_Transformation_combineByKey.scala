package com.at.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 19:59
 */
object Spark06_Transformation_combineByKey {

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val scoreRDD: RDD[(String, Int)] = sc.makeRDD(List(("jingjing",90),("jiafeng",60),("jingjing",96),("jiafeng",62),("jingjing",100),("jiafeng",50)))


    //求出每一个学生的平均成绩

/*
    //(jingjing,CompactBuffer(90, 96, 100))
    val groupByRDD: RDD[(String, Iterable[Int])] = scoreRDD.groupByKey()
    //(jingjing,(286,3))
    val res: RDD[(String, Double)] = groupByRDD.map {
      case (name, scores) => (name, scores.sum * 1.0 / scores.size)
    }

    如果分组之后某个组数据量比较大  会造成单点压力

*/


/*
    使用 reduceByKey 会在分区内Combiner 减小数据倾斜

    // 对RDD中的数据进行结构的转换 (jingjing,(90,1))
    val mapRDD: RDD[(String, (Int, Int))] = scoreRDD.map(kv => (kv._1, (kv._2, 1)))
    //通过reduceByKey对当前学生成绩进行聚合
    val reduceRDD: RDD[(String, (Int, Int))] = mapRDD.reduceByKey {
      case (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    }
    val res: RDD[(String, Double)] = reduceRDD.map {
      case (name, (scores, num)) => (name, scores * 1.0 / num)
    }

*/

    //createCombiner: V => C,     对RDD中当前key取出第一个value做一个初始化
    //mergeValue: (C, V) => C,    分区内计算规则，主要在分区内进行，将当前分区的value值，合并到初始化得到的c上面
    //mergeCombiners: (C, C) => C 分区间计算规则
      val combinRDD: RDD[(String, (Int, Int))] = scoreRDD.combineByKey(
      (_, 1),
      (t1: (Int, Int), v) => {
        (t1._1 + v, t1._2 + 1)
      },
      (t2: (Int, Int), t3: (Int, Int)) => {
        (t2._1 + t3._1, t2._2 + t3._2)
      }
    )
    val res: RDD[(String, Double)] = combinRDD.map {
      case (name, (scores, num)) => (name, scores * 1.0 / num)
    }

    res.collect().foreach(println)



    // 关闭连接
    sc.stop()

  }

  /*

   reduceByKey、aggregateByKey、foldByKey、combineByKey


    reduceByKey
      combineByKeyWithClassTag[V](
        (v: V) => v,                                第一个初始值不变
        func,
        func,
        partitioner)


    aggregateByKey
       combineByKeyWithClassTag[U](
        (v: V) => cleanedSeqOp(createZero(), v),    第一个初始值和分区内处理规则一致
        cleanedSeqOp,
        combOp,
        partitioner)

   foldByKey
      combineByKeyWithClassTag[V](
        (v: V) => cleanedFunc(createZero(), v),     分区内和分区间规则一致
        cleanedFunc,
        cleanedFunc,
        partitioner)

   combineByKey
       combineByKeyWithClassTag(
        createCombiner,                             把第一个值变成特定的结构
        mergeValue,
        mergeCombiners,
        partitioner,
        mapSideCombine,
        serializer)(null)


   */



}
