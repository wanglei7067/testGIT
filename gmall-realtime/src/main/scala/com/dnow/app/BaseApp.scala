package com.dnow.app

import org.apache.spark.streaming.StreamingContext

abstract class BaseApp {
  var context:StreamingContext = null
  var appName:String
  var internal:Int

  def runApp(op : =>Unit) ={
      try{
        op
        context.start()
        context.awaitTermination()
      }catch{
        case ex:Exception => println(ex.getMessage)
      }finally {

      }
  }

}
