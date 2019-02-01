package com.netflix.functions.persistence
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import org.apache.spark.sql.DataFrame


object InputDataFilterReader {

  case class Adult(age : Int, employer : String, salary : Long, education : String, educationyear : Int, maritalstatus : String, jobdesignation : String,
                   familystatus : String, colour : String, gender : String, points1 : Int, points2 : Int, points3: Int, country : String, salarytype : String)

  def workFlow(filepath : String, sc : SparkContext, spark : SparkSession) : DataFrame  = {

    val data  = sc.textFile(filepath).map(_.split(","))
      .map(p => Adult(p(0).trim.toInt, p(1).trim, p(2).trim.toLong, p(3).trim, p(4).trim.toInt, p(5).trim, p(6).trim, p(7).trim, p(8).trim, p(9).trim,
        p(10).trim.toInt, p(11).trim.toInt, p(12).trim.toInt, p(13).trim, p(14).trim))

    import spark.implicits._

    data.toDF.printSchema()

    print(data.getClass)

    data.saveToCassandra("sparktocassandra", "adultdatatable")

    //data.toDF.rdd[InputDataFilterReader.Adult]

    data.toDF
  }
}
