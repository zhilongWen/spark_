package com.at.spark.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-17 22:18
 */
object Spark03_TopN_req3 {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val actionRDD: RDD[UserVisitAction] = getActionRDD(sc)

    /*

        计算页面单跳转化率，什么是页面单跳转换率，比如一个用户在一次 Session 过程中访问的页面路径 3,5,7,9,10,21，
        那么页面 3 跳到页面 5 叫一次单跳，7-9 也叫一次单跳，那么单跳转化率就是要统计页面点击的概率

     */

    //通过页面的计数，计算每一个页面出现的总次数    作为求单跳转换率的分母
    val fmIds: Map[Long, Long] = actionRDD.map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap


    //将原始数据根据sessionId进行分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)

    //将分组后的数据按照时间进行升序排序
    val pageRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(
      datas => {
        //得到排序后的同一个session的用户访问行为
        val visitActions: List[UserVisitAction] = datas.toList.sortWith {
          (l, r) => {l.action_time < r.action_time}
        }

        //对排序后的用户访问行为进行结构转换，只保留页面就可以
        val pageIdsList: List[Long] = visitActions.map(_.page_id)

        //A->B->C->D->E->F
        //B->C->D->E->F
        //对当前会话用户访问页面 进行拉链  ,得到页面的流转情况 (页面A,页面B)
        val pageFlows: List[(Long, Long)] = pageIdsList.zip(pageIdsList.tail)

        //对拉链后的数据，进行结构的转换 (页面A-页面B,1)
        pageFlows.map {
          case (p1, p2) => {
            (p1 + "-" + p2, 1)
          }
        }
      }
    )

    //对页面跳转情况进行聚合操作
    val reduceRDD: RDD[(String, Int)] = pageRDD.map(_._2).flatMap(e => e).reduceByKey(_ + _)


    reduceRDD.foreach{
      case (pageFlow,fz) =>{
        val pageIds: Array[String] = pageFlow.split("-")

        //获取分母页面id
        val fmPageId: Long = pageIds(0).toLong
        //根据分母页面id，获取分母页面总访问数
        val fmSum: Long = fmIds.getOrElse(fmPageId, 1L)
        //转换率
          println(pageFlow +"--->" + fz.toDouble / fmSum)

      }
    }





    // 关闭连接
    sc.stop()



  }

}
