package org.sia.service

import org.apache.spark.sql.SparkSession
import sun.nio.cs.ext.EUC_TW.Encoder


object DataSetCreation {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataSet creation using custom object encoder")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .master("local").getOrCreate()
    val sc=spark.sparkContext

    //////////////////////////////////////

//    import spark.implicits._
//    val data = Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 19))
//    val ds = spark.createDataset(data)
//    ds.filter(x => (x.age > 20)).show()


    ////////////////////////////////////////////


//    import spark.implicits._
//    val personDS = Seq(Person("Max", 33), Person("Adam", 32), Person("Muller", 62)).toDS()
//    personDS.show()

    ////////////////////////////////////////

    //Creating Datasets from a RDD
//    import spark.implicits._
//    val rdd = sc.parallelize(Seq((1, "Spark"), (2, "Databricks")))
//    val integerDS = rdd.toDS()
//    integerDS.show()

    // Creating Datasets from a DataFrame
    import spark.implicits._
    val inputSeq = Seq(Company("ABC", 1998, 310), Company("XYZ", 1983, 904), Company("NOP", 2005, 83))
    val df = sc.parallelize(inputSeq).toDF()

    val companyDS = df.as[Company]
    companyDS.show()


  }

  case class Person(name: String, age: Int)

  case class Company(name: String, foundingYear: Int, numEmployees: Int)

}
