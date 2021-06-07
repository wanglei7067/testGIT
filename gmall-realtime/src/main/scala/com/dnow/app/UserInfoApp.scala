package com.dnow.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.common.constants.GmallConstants
import com.dnow.until.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{HostAndPort, JedisCluster}

object UserInfoApp extends BaseApp {
  override var appName: String = "userInfoApp"
  override var internal: Int = 10


  def main(args: Array[String]): Unit = {

    context =  new StreamingContext("local[2]",appName,Seconds(internal))
    runApp{
      // 1 消费kafka
      val ds: InputDStream[ConsumerRecord[String, String]]
      = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER_INFO, context)

      ds.map(_.value()).foreachRDD( rdd =>{
         rdd.foreachPartition( partition =>{
           val client: JedisCluster = new JedisCluster(new HostAndPort("194.168.1.103", 6379))
            // 数据缓存到redis中， key: userInfo-userId  value: jsonStr
           partition.foreach(jsonStr =>{
             val jSONObject: JSONObject = JSON.parseObject(jsonStr)
             client.set("userInfo-" + jSONObject.get("id"), jsonStr)
             println("userInfo-" + jSONObject.get("id"))
           })
           client.close()
         })
      })
    }
  }

}
