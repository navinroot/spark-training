package org.sia.chapter3App

import org.apache.spark.sql.SparkSession

import scala.io.Source.fromFile


object App {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()

    val sc = sparkSession.sparkContext

    val ghLog = sparkSession.read.json(args(0))

    val pushes = ghLog.filter("type = 'PushEvent'")

    val grouped = pushes.groupBy("actor.login").count

    val ordered = grouped.orderBy(grouped("count").desc)

    val employees = Set() ++ {
      for {
        line <- fromFile(args(1)).getLines()
      } yield line.trim
    }

    val bcEmployee =sc.broadcast(employees)

    import sparkSession.implicits._

    val isEmp = user => bcEmployee.value.contains(user)

    val sqlFunc =sparkSession.udf.register("setContainsUdf", isEmp)

    val filtered= ordered.filter(sqlFunc($"login"))

    filtered.write.format(args(3)).save(args(2))



  }


}
