package com.at.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-16 0:49
 */
object Spark02_TestSerializable {

  /*

    序列化
       -为什么要序列化
         因为在Spark程序中，算子相关的操作在Excutor上执行，算子之外的代码在Driver端执行，
         在执行有些算子的时候，需要使用到Driver里面定义的数据，这就涉及到了跨进程或者跨节点之间的通讯，
         所以要求传递给Excutor中的数组所属的类型必须实现Serializable接口
       -如何判断是否实现了序列化接口
         在作业job提交之前，其中有一行代码 val cleanF = sc.clean(f)，用于进行闭包检查
         之所以叫闭包检查，是因为在当前函数的内部访问了外部函数的变量，属于闭包的形式。
         如果算子的参数是函数的形式，都会存在这种情况
   */

  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf()
                            .setAppName("SparkApp")
                            .setMaster("local[*]")
//                            // 替换默认的序列化机制
//                            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                            // 注册需要使用 kryo 序列化的自定义类
//                            .registerKryoClasses(Array(classOf[Seacher]))

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val u1: User = new User()
    u1.name = "jingjing"
    val u2: User = new User()
    u2.name = "xiaoxiao"

    val rdd: RDD[User] = sc.parallelize(List(u1, u2))

//    rdd.foreach(println)
    rdd.foreach(user => println(user))

    // 关闭连接
    sc.stop()



  }

}

class User extends Serializable {

  var name:String = _

  override def toString: String = s"User($name)"

}

