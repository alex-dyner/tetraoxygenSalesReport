package com.tetraoxygen.reporting

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class HdfsCsvDataProvider[T <: Product : ClassTag : TypeTag] (sparkSession: SparkSession) {
  val sqlContext = sparkSession.sqlContext
  import sqlContext.implicits._

  def getRawDataFrame(storageMeta: StorageMeta): DataFrame = {
    val delimiter = storageMeta.getDelimiter()
    val salesSchema = storageMeta.getDataSchema()
    val saleDataPath = storageMeta.getDataPath()

    sparkSession
      .read
      .format("csv")
      .option("delimiter", delimiter)
      .option("header", "false")
      .option("inferSchema", "false")
      .schema(salesSchema)
      .load(saleDataPath)
  }

  def getRawDataset(storageMeta: StorageMeta): Dataset[T] = {
      getRawDataFrame(storageMeta)
      .as[T]
  }
}
