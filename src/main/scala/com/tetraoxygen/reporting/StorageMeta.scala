package com.tetraoxygen.reporting

import org.apache.spark.sql.types.StructType

trait StorageMeta {
  def getDelimiter(): String = ","

  def getDataSchema(): StructType

  def getDataPath(): String
}
