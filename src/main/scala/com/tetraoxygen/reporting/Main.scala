package com.tetraoxygen.reporting

import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging


object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {

    //Mode = {Top10SellingProductCategoryReport|Top10SellingCountry|Top10SellingProductByCategoryReport}
    val config: Config = ConfigFactory.load().getConfig("com.tetraoxygen.reporting")

    val processConfig: Config = config.getConfig("Process")
    val sparkAppName = processConfig.getString("sparkAppName");

    val sparkSession = SparkSession
      .builder()
      .appName(sparkAppName)
      .getOrCreate()

    val sourceConfig = config.getConfig("SourceInfo")
    val reportProcessor = new ReportProcessor(sparkSession, sourceConfig);

    val mode = processConfig.getString("mode")

    logger.debug("start compute report")
    val reportData = mode match {
      case "Top10SellingProductCategoryReport" => reportProcessor.doTop10SellingCategoryReport()
      case "Top10SellingCountry" => reportProcessor.doTop10SellingCountry()
      case "Top10SellingCategoryReport" => reportProcessor.doTop10SellingCategoryReport()
      case _ => throw new IllegalArgumentException("Unknown mode " + mode)
    }
    logger.debug("finish compute report")

    val reportDest = mode match {
      case "Top10SellingCategoryReport" => "sales_top10_categories"
      case "Top10SellingCountry" => "sales_top10_countries"
      case "Top10SellingProductCategoryReport" => "sales_top10_product_by_categories"
    }

    try {
      val jdbcConfig = config.getConfig("TargetInfo")

      val url = jdbcConfig.getString("url");

      val props = new java.util.Properties
      props.setProperty("driver", jdbcConfig.getString("driver"))
      props.setProperty("user", jdbcConfig.getString("user"))
      props.setProperty("password", jdbcConfig.getString("password"))

      val jdbcWriter = new JdbcReportDataWriter(url, props);
      logger.debug("start write to report database")
      jdbcWriter.write(reportDest, reportData)
      logger.debug("finish write to report database")
    }
    finally {
      sparkSession.stop()
    }
  }
}
