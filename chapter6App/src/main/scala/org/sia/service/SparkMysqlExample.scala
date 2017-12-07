package org.sia.service

import java.util.Properties

import com.databricks.spark.avro.AvroDataFrameWriter
import org.apache.spark.sql.SparkSession


object SparkMysqlExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark mysql connection and operation").getOrCreate()

    // configuration to use deflate compression
    spark.conf.set("spark.sql.avro.compression.codec", "deflate")
    spark.conf.set("spark.sql.avro.deflate.level", "5")

    //creating mysql properties
    val mysqlProps = new Properties()
    mysqlProps.setProperty("user", "root")
    mysqlProps.setProperty("password", "*******")

    val connectionUrl = "jdbc:mysql://localhost:3306/navin"


    val navinDf = spark.read.jdbc(connectionUrl, "employee", mysqlProps)

    navinDf.show()

    navinDf.createOrReplaceTempView("navin_table")

    spark.sql("select * from navin_table")

    navinDf.write.avro("output directory")

  }
}
