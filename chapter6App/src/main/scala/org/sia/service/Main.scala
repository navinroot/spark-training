package org.sia.service

import java.sql.Timestamp
import java.text.SimpleDateFormat

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.sia.VO.Order


/**
  * Created by nkumar on 11/24/2017.
  */
object Main {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("Local[4]").setAppName("Kafka Streaming using spark")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaRecieverParams = Map[String, String](
      "metadata.broker.list" -> "192.168.10.2:9092"
    )

    val kafkaStreams = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaRecieverParams, Set("orders")
    )

    val orders = kafkaStreams.flatMap(line => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val s = line._2.split(",")
      try {
        assert(s(6) == "B" || s(6) == "S")
        List(Order(new Timestamp(dateFormat.parse(s(0)).getTime()),
          s(1).toLong, s(2).toLong, s(3), s(4).toInt, s(5).toDouble, s(6) == "B"))
      }
      catch {
        case e: Throwable => println("Wrong line format (" + e + "): " + line)
          List()
      }
    }
    )

    val numPerType = orders.map(o => (o.buy, 0L)).reduceByKey(_ + _)

    val buySellList = numPerType.map(t =>
      if (t._1) ("BUYS", List(t._2.toString))
      else ("SELLS", List(t._2.toString))
    )

    val amountPerClient = orders.map(o => (o.clientId, o.amount * o.price))

    val amountState = amountPerClient.updateStateByKey((vals,
                                                        totalOpts: Option[Double]) => {
      totalOpts match {
        case Some(total) => Some(vals.sum + total)
        case none => Some(vals.sum)
      }
    })

    val top5Clients = amountState.transform(_.sortBy(_._2, false).map(_._1).zipWithIndex().filter(x => x._2 < 5))
      .repartition(1).map(x => x._1.toString).glom().map(arr => ("TOP5CLIENTS", arr.toList))

    val stocksPerWindow = orders.map(x => (x.symbol, x.amount)).reduceByKeyAndWindow(_ + _, Minutes(60))

    val topStocks = stocksPerWindow.transform(_.sortBy(_._2, false).map(_._1).zipWithIndex().filter(x => x._2 < 5)).repartition(1)
      .map(x => x._1.toString).glom().map(arr => ("TOP5CLIENTS", arr.toList))

    val finalStream = buySellList.union(top5Clients).union(topStocks)

    import org.sia.util.KafkaProducerWrapper

    finalStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        KafkaProducerWrapper.brokerList = "192.168.2.116:9092"
        val producer = KafkaProducerWrapper.instance
        iter.foreach({ case (metric, list) =>
          producer.send("metrics", metric, list.toString())
        })
      })
    })

    sc.setCheckpointDir("/home/spark/checkpoint")

    ssc.start()
    ssc.awaitTermination()
  }
}
