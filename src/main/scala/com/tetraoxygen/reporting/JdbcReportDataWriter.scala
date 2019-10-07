package com.tetraoxygen.reporting

import java.util.Properties

import org.apache.spark.sql.{Dataset, Row, SaveMode}

class JdbcReportDataWriter(url: String, prop: Properties) {

  def write(tablename: String, input: Dataset[Row]) ={
    input
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, tablename, prop)
  }
}
