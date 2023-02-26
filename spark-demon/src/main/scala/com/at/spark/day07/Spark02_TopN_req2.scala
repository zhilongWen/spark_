package com.at.spark.day07

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @author zero
 * @create 2021-03-17 21:16
 */
object Spark02_TopN_req2 {

  /*
      Top10热门品类中每个品类的Top10活跃Session统计
          对于排名前10的品类，分别获取每个品类点击次数排名前10的sessionId。（注意: 这里我们只关注点击次数，不关心下单和支付次数）
          这个就是说，对于top10的品类，每一个都要获取对它点击次数排名前10的sessionId。这个功能，
          可以让我们看到，对某个用户群体最感兴趣的品类，各个品类最感兴趣最典型的用户的session的行为。
   */
  def main(args: Array[String]): Unit = {


    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    val actionRDD: RDD[UserVisitAction] = getActionRDD(sc)


    //(16,CategoryCountInfo(16,1,0,0))
    val infoRDD: RDD[(String, CategoryCountInfo)] = actionRDD.flatMap(
      action => {
        if (action.click_category_id != -1) {
          //点击
          List((action.click_category_id.toString, CategoryCountInfo(action.click_category_id.toString, 1L, 0L, 0L)))
        } else if (action.order_category_ids != "null") {
          //下单
          val orderIds: Array[String] = action.order_category_ids.split(",")
          val categoryCountInfoesList: ListBuffer[(String, CategoryCountInfo)] = ListBuffer[(String, CategoryCountInfo)]()
          for (id <- orderIds) {
            categoryCountInfoesList.append((id.toString, CategoryCountInfo(id.toString, 0L, 1L, 0L)))
          }
          categoryCountInfoesList
        } else if (action.pay_category_ids != "null") {
          //支付
          val ids: Array[String] = action.pay_category_ids.split(",")
          val infoes: ListBuffer[(String, CategoryCountInfo)] = ListBuffer[(String, CategoryCountInfo)]()
          for (id <- ids) {
            infoes.append((id.toString, CategoryCountInfo(id.toString, 0L, 0L, 1L)))
          }
          infoes
        } else {
          Nil
        }
      }
    )

    //(4,CategoryCountInfo(4,5961,1760,1271)) => CategoryCountInfo(4,5961,1760,1271)
    val mapRDD: RDD[CategoryCountInfo] = infoRDD.reduceByKey(
      (info1, info2) => {
        info1.clickCount = info1.clickCount + info2.clickCount
        info1.payCount = info1.payCount + info2.payCount
        info1.orderCount = info1.orderCount + info2.orderCount
        info1
      }
    ).map(_._2)

    val top10: Array[CategoryCountInfo] = mapRDD.sortBy(info => (info.clickCount, info.orderCount, info.payCount),false).collect().take(10)


    val ids: Array[String] = top10.map(_.categoryId)

    //ids可以进行优化， 因为发送给Excutor中的Task使用，每一个Task都会创建一个副本，所以可以使用广播变量
    val broadcastIds: Broadcast[Array[String]] = sc.broadcast(ids)

    // 将原始数据进行过滤（1.保留热门品类 2.只保留点击操作）
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = actionRDD.filter(
      action => {
        if (action.click_category_id != -1) {
          //热门品类的点击
          broadcastIds.value.contains(action.click_category_id.toString)
        } else {
          false
        }
      }
    ).map(
      action => {
        (action.click_category_id + "_" + action.session_id, 1)
      }
    ).reduceByKey(_ + _).map(
      //(click_category_id,(action.session_id,sum))
      kv => (kv._1.split("_")(0), (kv._1.split("_")(1), kv._2))
    ).groupByKey()

    val res: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      datas => {
        datas.toList.sortWith {
          case (l, r) => {
            l._2 > r._2
          }
        }.take(10)
      }
    )


    res.foreach(println)




    // 关闭连接
    sc.stop()


  }

}


