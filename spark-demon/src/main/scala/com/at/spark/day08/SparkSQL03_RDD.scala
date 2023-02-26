package com.at.spark.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-21 14:50
 */
object SparkSQL03_RDD {


  def main(args: Array[String]): Unit = {


    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("jingjing",20),("bangzhang",40),("xuchong",30)))

    //求平均年龄----RDD算子方式实现
    /*
    val res: (Int, Int) = rdd.map {
      case (name, age) => {
        (age, 1)
      }
    }.reduce {
      case (f1, f2) => {
        (f1._1 + f2._1, f1._2 + f2._2)
      }
    }

    println(res._1.toDouble / res._2)
    */

    val myAcc = new MyAccumulator()

    sc.register(myAcc)

    rdd.foreach{
      case (name,age) =>{
        myAcc.add(age)
      }
    }

    println("myAcc:"+myAcc.value)


  }

}


//求平均年龄----通过累加器实现
class MyAccumulator extends AccumulatorV2[Int,Double]{

  var ageSum:Int = 0
  var countSum:Int = 0

  override def isZero: Boolean = {
    ageSum == 0 && countSum == 0
  }

  override def copy(): AccumulatorV2[Int, Double] = {
    var myAccc = new MyAccumulator()
    myAccc.ageSum = this.ageSum
    myAccc.countSum = this.countSum
    myAccc
  }

  override def reset(): Unit = {
    ageSum = 0
    countSum = 0
  }

  override def add(v: Int): Unit = {
    ageSum += v
    countSum += 1
  }

  override def merge(other: AccumulatorV2[Int, Double]): Unit = {
    other match {
      case mc:MyAccumulator => {
        this.ageSum += mc.ageSum
        this.countSum += mc.countSum
      }
      case _ =>
    }
  }

  override def value: Double = ageSum.toDouble/countSum
}


