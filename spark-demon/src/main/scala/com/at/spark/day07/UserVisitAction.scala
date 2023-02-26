package com.at.spark.day07

/**
 * @author zero
 * @create 2021-03-17 19:33
 */
//用户访问动作表
case class UserVisitAction(date: String,//用户点击行为的日期
                           user_id: Long,//用户的ID
                           session_id: String,//Session的ID
                           page_id: Long,//某个页面的ID
                           action_time: String,//动作的时间点

                           search_keyword: String,//用户搜索的关键词 不是搜索行为为 “null”

                           click_category_id: Long,//某一个商品品类的ID  不是点击行为为 -1
                           click_product_id: Long,//某一个商品的ID   不是点击行为为 -1

                           order_category_ids: String,//一次订单中所有品类的ID集合  不是下单行为为 “null”
                           order_product_ids: String,//一次订单中所有商品的ID集合 不是下单行为为 “null”

                           pay_category_ids: String,//一次支付中所有品类的ID集合  不是支付行为为 “null”
                           pay_product_ids: String,//一次支付中所有商品的ID集合 不是支付行为为 “null”

                           city_id: Long)//城市 id

