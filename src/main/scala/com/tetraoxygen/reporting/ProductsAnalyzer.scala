package com.tetraoxygen.reporting

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.functions.col

object ProductsAnalyzer {
  def getTop10ProductByCategory(rawSalesData: DataFrame): Dataset[Row] = {
    val groupedSalesByProduct: DataFrame = rawSalesData
      .groupBy("product_category", "product_name")
      .count()

    val sortColumn = col("count").desc
    val byProdCategory = Window.partitionBy("product_category").orderBy(sortColumn)
    val rankedProductSalesByCategory = groupedSalesByProduct
      .withColumn("product_sales_rank", rank() over byProdCategory)

    rankedProductSalesByCategory
      .filter(r => r.getAs[Int]("product_sales_rank") <= 10)
  }

  def getTop10Category(rawSalesData: DataFrame): Dataset[Row] = {
    val groupColumn = col("product_category")
    val qtyColumnName = "qty"
    val numberOfTopRows = 10

    rawSalesData
      .groupBy(groupColumn)
      .count()
      .withColumnRenamed("count", qtyColumnName)
      .orderBy(col(qtyColumnName).desc)
      .limit(numberOfTopRows)
  }
}
