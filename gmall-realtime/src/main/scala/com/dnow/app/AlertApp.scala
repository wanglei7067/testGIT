package com.dnow.app

import com.alibaba.fastjson.JSON
import com.common.constants.GmallConstants
import com.dnow.bean.{CouponAlertInfo, EventLog}
import com.dnow.until.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}


object AlertApp extends BaseApp {
  override var appName: String = "alertApp"
  override var internal: Int = 10

  def main(args: Array[String]): Unit = {

  context =  new StreamingContext("local[2]",appName,Seconds(internal))
  runApp{
    // 1 消费kafka
    val ds: InputDStream[ConsumerRecord[String, String]]
    = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, context)

    val logDs: DStream[EventLog] = ds.map(record => {
      val log: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
      val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(log.ts), ZoneId.of("Asia/Shanghai"))
      log.logDate = dateTime.format(formatter)
      log.logHour = dateTime.getHour.toString
      log
    })

    import scala.util.control.Breaks._
    logDs.window(Minutes(5))
      .map( log =>  ((log.mid,log.uid),log))
      .groupByKey()
      .map{
        case ((mid,uid), logs) =>{
           var needAlert:Boolean = false
           breakable {
             logs.foreach(log => {
               if ("clickItem".equals(log.evid)) {
                 needAlert = false
                 break()
               } else if ("coupon".equals(log.evid)) {
                 needAlert = true
               }
             })
           }
           if(needAlert){
             (mid,logs)
           }else{
             (null,null)
           }
        }
      }
      .filter(_._1 != null )
      .groupByKey()
      .filter(_._2.size > 3)
      .mapValues(_.flatten)
      .map{
        case (mid, logs) =>{
          var uids:java.util.HashSet[String] = new java.util.HashSet[String]
          var itemIds:java.util.HashSet[String] = new java.util.HashSet[String]
          var events:java.util.List[String] = new java.util.ArrayList[String]

          logs.foreach( log =>{
             uids.add(log.uid)
             itemIds.add(log.itemid)
             events.add(log.evid)
          })
          CouponAlertInfo(mid,uids,itemIds,events,System.currentTimeMillis())
        }
      }
      .map(couponAlertInfo =>{
        val formatter: DateTimeFormatter =
                       DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
        val dateTime: LocalDateTime = LocalDateTime.ofInstant(
                                        Instant.ofEpochSecond(couponAlertInfo.ts),
                                        ZoneId.of("Asia/Shanghai"))
        // 用mid+"yyyy-MM-dd HH:mm" 作为主键id, 实现一分钟只有一条预警记录的效果
        (couponAlertInfo.mid + dateTime.format(formatter) , couponAlertInfo)
      })
      .foreachRDD(rdd =>{
        rdd.foreachPartition( partition =>{
          MyEsUtil.insertBulk(
            "gmall_coupon_alert_"+LocalDate.now(),
                        partition.toList)
        })
      })


  }

  }

}
