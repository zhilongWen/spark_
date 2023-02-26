package com.at.spark.day07

/**
 * @author zero
 * @create 2021-03-17 19:33
 */
// 输出结果表
case class CategoryCountInfo(categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long)//支付次数
