package com.dnow.test

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

object Demo {


  def main(args: Array[String]): Unit = {

    println(LocalDate.now())


    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(1622208592), ZoneId.of("Asia/Shanghai"))

    println(dateTime.format(formatter))
    println(dateTime.getHour.toString)


    println( LocalDate.now())

  }

}
