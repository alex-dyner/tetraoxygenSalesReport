package com.tetraoxygen.reporting

import com.typesafe.config.Config
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class SaleStorageMeta(sourceInfoConfig: Config)  extends StorageMeta {
  override def getDataSchema(): StructType = {
    StructType(
      Array(
        StructField("product_name", StringType, true),
        StructField("price", DoubleType, true),
        StructField("sale_tm", StringType, true),
        StructField("product_category", StringType, true),
        StructField("ip_address", StringType, true)
      )
    )
  }

  override def getDataPath(): String = sourceInfoConfig.getString("salesDataPath")
}
