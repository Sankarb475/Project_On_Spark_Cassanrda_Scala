package com.netflix.functions.UserDefinedFunctions

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object UserDefinedFunction {

  def totalSalaryCountryWise(data : DataFrame) : DataFrame = {
    val a = data.groupBy("country")
      .agg(sum("Salary").alias("Salary"))
      .select("country","Salary")
    a
  }

  def countOfBachelor(data : DataFrame, spark : SparkSession) : DataFrame = {
    import spark.implicits._
    val a = data.where($"education" === "Bachelors")
      .filter($"salaryType" === ">50K")
      .groupBy("country")
      .agg(count("education").alias("Count"))
      .select("country","Count")
    a
  }

}
