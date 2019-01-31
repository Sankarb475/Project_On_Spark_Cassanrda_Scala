package com.netflix.functions.persistence
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object InputDataFilterReader {

  case class Adult(Age : Int, Employer : String, Salary : Long, education : String, educationYear : Int, maritalStatus : String, jobDesiognation : String,
                   familyStatus : String, colour : String, gender : String, Points1 : Int, Points2 : Int, Points3: Int, country : String, salaryType : String)

  def workFlow(filepath : String, sc : SparkContext, spark : SparkSession) : DataFrame = {

    println("reached here")

    val data  = sc.textFile(filepath).map(_.split(",")).map(p => Adult(p(0).trim.toInt, p(1).trim, p(2).trim.toLong, p(3).trim, p(4).trim.toInt, p(5).trim, p(6).trim, p(7).trim, p(8).trim, p(9).trim, p(10).trim.toInt, p(11).trim.toInt, p(12).trim.toInt, p(13).trim, p(14).trim))

    //println(data.getClass)
    import spark.implicits._
    data.toDF
  }
}
