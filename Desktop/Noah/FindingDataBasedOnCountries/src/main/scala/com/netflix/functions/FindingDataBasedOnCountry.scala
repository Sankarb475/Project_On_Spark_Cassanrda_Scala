package com.netflix.functions

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import com.netflix.functions.persistence.InputDataFilterReader
import com.netflix.functions.UserDefinedFunctions.UserDefinedFunction._
import org.apache.spark.sql.DataFrame
import com.netflix.functions.WrtingToCassandra.CassandraWrite._
import org.apache.spark.rdd.RDD

import com.datastax.driver.core.{ResultSet, Row}
import com.datastax.spark.connector._
//  import org.slf4j.{Logger, LoggerFactory}


object FindingDataBasedOnCountry {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("APP_NAME")
      .setMaster("local")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.auth.username", "")
      .set("spark.cassandra.auth.password", "")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val sc: SparkContext = spark.sparkContext

    //checking the input parameter that is the input file path
    if (args.length != 1) {
      System.exit(1)
    }

    val filepath: String = args(0)

    val data : DataFrame = InputDataFilterReader.workFlow(filepath,sc,spark)
    //writingAdultDataTable(data)                                                       //writing to cassandra table


    //country wise total salary
    var input : DataFrame = data.select("country","salary")
    val out1 : DataFrame = totalSalaryCountryWise(input)
    writingCountryWiseSalaryTable(out1)
    out1.show

    //Country wise count of bachelor with salary type > 50K
    input = data.select("education","country", "salarytype")
    val out2 : DataFrame = countOfBachelor(input,spark)
    writingCountryWiseBachelorTable(out2)
    out2.show

    //top 15 lowest Average age country
    input = data.select("country","age")
    val out3 : DataFrame = avgAgeCountryWise(input)
    writingCountryWiseAvgAgeTable(out3)
    out3.orderBy("Avg Age").limit(15).show

    //grouping people who has been divorced
    input = data.filter($"maritalstatus" === "Divorced").select("country", "gender", "education", "maritalstatus", "age")
    //input.printSchema()
    val out4 : DataFrame = divorceDetails(input)
    writingDivorceDataTable(out4)
    out4.show

  }
}
