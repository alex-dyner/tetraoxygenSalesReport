package com.tetraoxygen.reporting

import com.tetraoxygen.reporting.domain.RawGeoInfo
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, udf}

object GeoAnalyzer {
  private def parseAddress(ipAddress: String): Array[Byte] = {
    try {
      ipAddress.split("\\.").map(s => (s.toInt).toByte)
    }
    catch {
      case x: Exception => {}
      // TODO: add accumulator for unparsed ip-address
      null
    }
  }

  private def matchIp(ipAddress: String, network: String): Boolean ={
    val netAddressAndPrefix = network.split("/")
    val netIpAddress = netAddressAndPrefix(0)
    val netIpAddressSegm: Array[Byte] = parseAddress(netIpAddress)
    val netMaskBits = netAddressAndPrefix(1).toInt

    val ipAddressSegm: Array[Byte] = parseAddress(ipAddress)
    val finalByte = (0xFF00 >> (netMaskBits & 0x07)).toByte
    val netMaskFullBytesCount = netMaskBits / 8

    for (i <- 0 until netMaskFullBytesCount) {
      if (ipAddressSegm(i) != netIpAddressSegm(i))
        return false
    }

    if (finalByte != 0)
      return (ipAddressSegm(netMaskFullBytesCount) & finalByte) == (netIpAddressSegm(netMaskFullBytesCount) & finalByte)

    true
  }

  def getTop10CountriesByTotalSales(rawSalesData: DataFrame, ipGeoData: Dataset[RawGeoInfo]): Dataset[Row] = {
    val finalAggrColumnName = "total_amount"
    val countOfTopRows = 10

    val matchIP = udf(matchIp _)

    rawSalesData
      .crossJoin(ipGeoData)

      .withColumn("isCountryMatchColumnName", matchIP(col("ip_address"), col("network")))
      .filter(col("isCountryMatchColumnName"))

      .groupBy(col("countryName"))
      .sum("price")

      .withColumnRenamed("sum(price)", finalAggrColumnName)
      .sort(col(finalAggrColumnName).desc)
      .limit(countOfTopRows)

  }
}
