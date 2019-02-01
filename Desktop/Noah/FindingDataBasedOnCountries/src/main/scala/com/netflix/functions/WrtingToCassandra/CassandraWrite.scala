package com.netflix.functions.WrtingToCassandra


import org.apache.spark.sql.DataFrame
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.SqlRowWriter
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.sql.Row

object CassandraWrite {

  implicit val rowWriter : RowWriterFactory[Row] = SqlRowWriter.Factory


  //writing the full data into cassandra table AdultDataTable
  def writingAdultDataTable(data : DataFrame) : Unit = {
    data.rdd.saveToCassandra("sparktocassandra", "adultdatatable")

  }


  //writing the full data into cassandra table CountryWiseSalaryTable
  def writingCountryWiseSalaryTable(data : DataFrame) : Unit = {

    print("here 232")
    data.rdd.saveToCassandra("sparktocassandra", "countrywisesalarytable")
  }


  //writing the full data into cassandra table CountryWiseSalaryTable
  def writingCountryWiseBachelorTable(data : DataFrame) : Unit = {

    print("here 233")
    data.rdd.saveToCassandra("sparktocassandra", "countrywisebachelortable")
  }


  //writing the full data into cassandra table CountryWiseSalaryTable
  def writingCountryWiseAvgAgeTable(data : DataFrame) : Unit = {

    print("here 234")
    data.rdd.saveToCassandra("sparktocassandra", "countrywiseavgagetable")
  }


  //writing the full data into cassandra table CountryWiseSalaryTable
  def writingDivorceDataTable(data : DataFrame) : Unit = {

    print("here 235")
    data.rdd.saveToCassandra("sparktocassandra", "divorcedatatable")
  }

}
