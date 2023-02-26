package com.at.spark.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @author zero
 * @create 2021-03-17 19:28
 */
object Spark01_TopN_req1 {

  def main(args: Array[String]): Unit = {


    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /*
        每个品类的点击、下单、支付的量来统计热门品类
     */

    val actionRDD: RDD[UserVisitAction] = getActionRDD(sc)

    //3.判断当前这条日志记录的是什么行为，并且封装为结果对象    (品类,点击数,下单数,支付数)==>例如：如果是鞋的点击行为  (鞋,1,0,0)
    //(鞋,1,0,0)
    //(保健品,1,0,0)
    //(鞋,0,1,0)
    //(保健品,0,1,0)
    //(鞋,0,0,1)=====>(鞋,1,1,1)
    val infoRDD: RDD[CategoryCountInfo] = actionRDD.flatMap(
      action => {
        if (action.click_category_id != -1) {
          //点击
          List(CategoryCountInfo(action.click_category_id + "", 1, 0, 0))
        } else if (action.order_category_ids != "null") {
          //下单
          val orderIds: Array[String] = action.order_category_ids.split(",")
          val list: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()
          for (id <- orderIds) {
            list.append(CategoryCountInfo(id + "", 0L, 1L, 0L))
          }
          list

        } else if (action.pay_category_ids != "null") {
          //支付
          val payIds: Array[String] = action.pay_category_ids.split(",")
          val list: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()
          for (id <- payIds) {
            list.append(CategoryCountInfo(id + "", 0L, 0L, 1L))
          }
          list
        } else {
          Nil
        }
      }
    )

    //将相同品类的放到一组
    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = infoRDD.groupBy(_.categoryId)

    //将分组之后的数据进行聚合处理    (鞋,100,90,80)
    val reduceRDD: RDD[(String, CategoryCountInfo)] = groupRDD.mapValues(
      datas => {
        datas.reduce {
          (info1, info2) => {
            info1.clickCount = info1.clickCount + info2.clickCount
            info1.orderCount = info1.orderCount + info2.orderCount
            info1.payCount = info1.payCount + info2.payCount
            info1
          }
        }
      }
    )

//    对上述RDD的结构进行转换，只保留value部分  ,得到聚合之后的RDD[CategoryCountInfo]
    val mapRDD: RDD[CategoryCountInfo] = reduceRDD.map(_._2)

    //对RDD中的数据排序，取前10
    val res: Array[CategoryCountInfo] = mapRDD.sortBy(info => (info.clickCount, info.orderCount, info.payCount), false).take(10)

    res.foreach(println)


    // 关闭连接
    sc.stop()

  }

}





