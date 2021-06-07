package com.dnow.app

import com.alibaba.fastjson.JSON
import com.common.constants.GmallConstants
import com.dnow.bean.OrderInfo
import com.dnow.until.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object GmvApp  extends BaseApp {
  override var appName: String = "local[2]"
  override var internal: Int = 10

  context =  new StreamingContext("local[2]",appName,Seconds(internal))
  //  context.sparkContext.setLogLevel("error")

  def main(args: Array[String]): Unit = {

    runApp{

      // 1 消费kafka
      val ds: InputDStream[ConsumerRecord[String, String]]
      = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_NEW_ORDER, context)

      val insertDS: DStream[OrderInfo] = ds.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val localDateTime: LocalDateTime = LocalDateTime.parse(orderInfo.create_time, formatter)
        orderInfo.create_hour = localDateTime.getHour.toString

        val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        orderInfo.create_date = localDateTime.format(dateFormatter)

        //模拟手机号脱敏
        orderInfo.consignee_tel = orderInfo.consignee_tel.replaceAll("(\\w{3})\\w*(\\w{4})", "$1****$2")
        orderInfo
      })

      // 6 数据写入hbase
      //导入phoenix提供的各种静态方法
      import org.apache.phoenix.spark._
      insertDS.foreachRDD(rdd =>{
        //把数据写入hbase+phoenix
        rdd.saveToPhoenix("GMALL_ORDER_INFO",
          Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
          HBaseConfiguration.create(),
          Some("hadoop102,hadoop103,hadoop104:2181"))
      })
    }
  }

}
