package com.tetraoxygen.reporting

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

class ProductsAnalyzerGet10CategSuite extends FunSuite with BeforeAndAfter {

  var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  test("getTop10Category") {
    val rawData = Seq(
      Row("A1", 12.21, "2019-09-10 12:12:12", "C1", "10.7.7.7"),
      Row("A1", 12.21, "2019-09-10 12:12:12", "C1", "10.7.7.7"),
      Row("A3", 20.21, "2019-09-10 12:12:12", "C1", "10.7.7.7"),
      Row("X2", 13.21, "2019-09-10 12:12:12", "C2", "10.7.7.4"),
    )

    val schema = (new SaleStorageMeta(null).getDataSchema())

    val rowsRdd: RDD[Row] = spark.sparkContext.parallelize(rawData)
    val df = spark.createDataFrame(rowsRdd, schema)

    val result = ProductsAnalyzer.getTop10Category(df).collect()
    assert(result.length === 2)

    val mappedResult = result.map(r => (r.getString(0), r.getLong(1))).toMap
    assert(mappedResult("C1") === 3)
    assert(mappedResult("C2") === 1)
  }

  after {
    spark.stop()
  }
}