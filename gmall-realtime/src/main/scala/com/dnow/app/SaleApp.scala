package com.dnow.app

import com.alibaba.fastjson.JSON
import com.common.constants.GmallConstants
import com.dnow.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.dnow.until.{MyEsUtil, MyKafkaUtil}
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{HostAndPort, JedisCluster}
import java.time.{LocalDate}
import java.util
import scala.collection.mutable.ListBuffer

object SaleApp extends BaseApp {
  override var appName: String = "userInfoApp"
  override var internal: Int = 10


  def main(args: Array[String]): Unit = {

    context =  new StreamingContext("local[2]",appName,Seconds(internal))

    runApp{
      // 1 消费kafka
      val orderInfoDs0: InputDStream[ConsumerRecord[String, String]]
      = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_INFO, context)

      val orderDetailDs0: InputDStream[ConsumerRecord[String, String]]
      = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, context)

      val orderInfoDs: DStream[(String, OrderInfo)] = orderInfoDs0.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        //模拟手机号脱敏
        orderInfo.consignee_tel = orderInfo.consignee_tel.replaceAll("(\\w{3})\\w*(\\w{4})", "$1****$2")
        (orderInfo.id, orderInfo)
      })

      val orderDetailDs: DStream[(String, OrderDetail)] = orderDetailDs0.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })

      val joinDs: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDs.fullOuterJoin(orderDetailDs)

      val joinProcessResultDs: DStream[SaleDetail] = joinDs.mapPartitions(partition => {

        val client: JedisCluster = new JedisCluster(new HostAndPort("192.168.1.102", 6380))
        val saveList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()

        partition.foreach {
          case (orderId, (orderInfoOption, orderDetailOption)) => {
            val gson = new Gson()

            if (orderInfoOption != None) {
              val orderInfo: OrderInfo = orderInfoOption.get
              // 匹配上的
              if (orderDetailOption != None) {
                val orderDetail: OrderDetail = orderDetailOption.get
                saveList.append(new SaleDetail(orderInfo, orderDetail))
              }

              // 匹配缓存中的orderDetail orderDetail-orderId
              val orderDetailSet: util.Set[String] = client.smembers("orderDetail-" + orderId)
              orderDetailSet.forEach(jsonStr => {
                val detail: OrderDetail = JSON.parseObject(jsonStr, classOf[OrderDetail])
                saveList.append(new SaleDetail(orderInfo, detail))
              })

              // 将orderInfo缓存到redis中 orderInfo-orderId
              // 设置2倍的最大处理时间差
              client.setex("orderInfo-" + orderId, 2 * 5 * 60, gson.toJson(orderInfo))
            } else {

              val orderDetail: OrderDetail = orderDetailOption.get
              val jsonStr: String = client.get("orderInfo-" + orderId)
              if (jsonStr != null) { // orderDetail迟到的情况,直接匹配缓存的OrderInfo
                val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
                saveList.append(new SaleDetail(orderInfo, orderDetail))
              } else { // orderDetail 早到的情况，缓存到redis中，结构 k:orderId, Set[OrderDetail]
                client.sadd("orderDetail-" + orderId, gson.toJson(orderDetail))
                client.expire("orderDetail-" + orderId,2 * 5 * 60) // 设置过期时间
              }
            }
          }
        }
          client.close()
        saveList.iterator
      })

      joinProcessResultDs
        .mapPartitions( partition =>{
        val client: JedisCluster = new JedisCluster(new HostAndPort("192.168.1.103", 6379))
        val mapResult: Iterator[(String, SaleDetail)] = partition.map(item => {
          val userInfoJsonStr: String = client.get("userInfo-" + item.user_id)
          val userInfo: UserInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])
          item.mergeUserInfo(userInfo)
          (item.order_detail_id, item)
        })
          client.close()
        mapResult
        })
        .foreachRDD(rdd =>{
          rdd.foreachPartition(partition =>{
            MyEsUtil.insertBulk("gmall_sale_detail_"+LocalDate.now(),partition.toList)
          })
        })
    }
  }

}
