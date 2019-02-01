package com.netflix.functions.UserDefinedFunctions

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object UserDefinedFunction {

  def totalSalaryCountryWise(data : DataFrame) : DataFrame = {
    val a = data.groupBy("country")
      .agg(sum("salary").alias("salary"))
      .select("country","salary")
    a
  }

  def countOfBachelor(data : DataFrame, spark : SparkSession) : DataFrame = {
    import spark.implicits._
    val a = data.where($"education" === "Bachelors")
      .filter($"salaryType" === ">50K")
      .groupBy("country")
      .agg(count("education").alias("counting"))
      .select("country","counting")
    a
  }

  def avgAgeCountryWise(data : DataFrame) : DataFrame = {
    val a = data.groupBy("country")
      .agg(avg("Age").alias("Avg Age"))
      .select("country","Avg Age")
    a
  }

  def divorceDetails(data : DataFrame) : DataFrame = {
    val a = data.groupBy("country", "age", "gender")
      .agg(count("maritalstatus").alias("maritalstatus"), avg("age").alias("avgage"))
      .select("country","avgage", "maritalstatus", "gender")
    a.printSchema()
    a
  }

}
