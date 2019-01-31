package com.netflix.functions

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import com.netflix.functions.persistence.InputDataFilterReader
import com.netflix.functions.UserDefinedFunctions.UserDefinedFunction._
import org.apache.spark.sql.DataFrame

object FindingDataBasedOnCountry {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("APP_NAME")
      .setMaster("local")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.auth.username", "")
      .set("spark.cassandra.auth.password", "")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sc: SparkContext = spark.sparkContext

    //checking the input parameter that is the input file path
    if (args.length != 1) {
      System.exit(1)
    }

    val filepath: String = args(0)

    val data = InputDataFilterReader.workFlow(filepath,sc,spark)

    //country wise total salary
    var input : DataFrame = data.select("Country","Salary")

    val out1 : DataFrame = totalSalaryCountryWise(input)

    out1.show

    //Country wise count of bachelor with salary type > 50K

    input = data.select("education","country", "salaryType")

    val out2 : DataFrame = countOfBachelor(input,spark)

    out2.show

  }
}
