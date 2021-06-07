package com.dnow.app

import com.alibaba.fastjson.JSON
import com.common.constants.GmallConstants
import com.dnow.bean.StartUpLog
import com.dnow.until.{MyKafkaUtil, PropertiesUtil, RedisUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util
import java.util.Properties

object DauApp extends BaseApp{
  override var appName: String = "dauApp"
  override var internal: Int = 5
  val config: Properties = PropertiesUtil.load("config.properties")
  private val logger: Logger = LoggerFactory.getLogger(DauApp.getClass)



  def main(args: Array[String]): Unit = {

    context =  new StreamingContext("local[2]",appName,Seconds(internal))
    context.sparkContext.setLogLevel("error")


    runApp{
      // 1 消费kafka
      val ds: InputDStream[ConsumerRecord[String, String]]
              = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, context)

      // 2 数据流 转换 结构变成case class 补充两个时间字段
      val ds1: DStream[StartUpLog] = ds.map(record => {
        val jsonStr: String = record.value()
        val startUpLog: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])

        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(startUpLog.ts), ZoneId.of("Asia/Shanghai"))

        startUpLog.logDate = dateTime.format(formatter)
        startUpLog.logHour = dateTime.getHour.toString
        startUpLog
      })

      //  3 批次内去重  key：  logDate_mid_id value:   StartUpLog
       // 先将key相同的分到一组，在当前组中取ts最小的

      val distinctRdd: DStream[StartUpLog] = ds1.transform(rdd => {
        rdd.map(itemLog => (itemLog.logDate + itemLog.mid, itemLog))
          .groupByKey()
          .flatMap { case (key, iterable) => iterable.toList.sortBy(_.ts).take(1) }
      })

      //   以下是每次都广播的写法  一个批次(Job)只需要广播一次
      val newItemDs: DStream[StartUpLog] = distinctRdd.transform(rdd => {
        // 4  从redis中查询之前的状态
        //  driver 按周期执行
        val client: JedisCluster = new JedisCluster(new HostAndPort("192.168.1.102", 6379))

        val existSet: util.Set[String] = client.smembers("DAU:" + LocalDate.now())
        logger.error(" ############> existKey for redis is :" + "DAU:" + LocalDate.now())

        client.close()
        val bc: Broadcast[util.Set[String]] = context.sparkContext.broadcast(existSet)
        val filtRdd: RDD[StartUpLog] = rdd.filter(startUplog => !bc.value.contains(startUplog.logDate + startUplog.mid))

        filtRdd
      })

      // 后面重复用到该rdd，缓存一下。
      newItemDs.cache()

      // 5 保持状态到redis
      newItemDs.foreachRDD(rdd =>{
        rdd.foreachPartition( partition =>{
          //driver 按周期执行
//          val client: JedisCluster = new JedisCluster(new HostAndPort("192.168.1.102", 6379))
          val client: Jedis = RedisUtil.getJedisClient
          partition.foreach( log => {
            client.sadd("DAU:" + log.logDate, log.mid)
            logger.error("========> redis key is :" + "DAU:" + log.logDate)
          })

          client.close()
        })
      })

      // 6 数据写入hbase
      //导入phoenix提供的各种静态方法
      import org.apache.phoenix.spark._
      newItemDs.foreachRDD(rdd =>{
         //把数据写入hbase+phoenix
          rdd.saveToPhoenix("gmall_dau",
                            Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
                            HBaseConfiguration.create(),
                            Some("hadoop102,hadoop103,hadoop104:2181"))
      })
    }
  }

}
