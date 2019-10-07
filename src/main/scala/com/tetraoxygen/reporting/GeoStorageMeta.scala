package com.tetraoxygen.reporting

import com.typesafe.config.Config
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class GeoStorageMeta(sourceInfoConfig: Config) extends StorageMeta{

  override def getDataSchema() = {
    StructType(
      Array(
        StructField("network", StringType, true),
        StructField("countryName", StringType, true),
      )
    )
  }

  override def getDataPath(): String = sourceInfoConfig.getString("geoDataPath")
}
