package org.sia.VO

/**
  * Created by nkumar on 11/24/2017.
  */
case class Order(time:java.sql.Timestamp, orderId:Long, clientId:Long, symbol: String, amount: Int, price: Double , buy:Boolean)
