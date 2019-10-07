package com.tetraoxygen.reporting

import com.tetraoxygen.reporting.domain.RawGeoInfo
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.typesafe.config.Config

class ReportProcessor (sparkSession: SparkSession, config: Config) {

  def doTop10SellingCategoryReport(): Dataset[Row] = {
    val rawSalesData = getSalesData()
    ProductsAnalyzer.getTop10Category(rawSalesData)
  }

  def doTop10SellingCountry(): Dataset[Row] = {
    val rawSalesData = getSalesData()
    val rawGeoData = getGeoDataset()
    GeoAnalyzer.getTop10CountriesByTotalSales(rawSalesData, rawGeoData)
  }

  def doTop10SellingProductByCategoryReport(): Dataset[Row] = {
    val rawSalesData = getSalesData()
    ProductsAnalyzer.getTop10ProductByCategory(rawSalesData)
  }

  private def getGeoDataset(): Dataset[RawGeoInfo] = {
    val geoMeta = new GeoStorageMeta(config)
    val hdfs = new HdfsCsvDataProvider[RawGeoInfo](sparkSession)
    hdfs.getRawDataset(geoMeta)
  }

  private def getSalesData(): DataFrame = getData(new SaleStorageMeta(config))

  private def getGeoData(): DataFrame = getData(new GeoStorageMeta(config))

  private def getData(storageMeta: StorageMeta): DataFrame = {
    val hdfs = new HdfsCsvDataProvider(sparkSession)
    hdfs.getRawDataFrame(storageMeta)
  }
}
