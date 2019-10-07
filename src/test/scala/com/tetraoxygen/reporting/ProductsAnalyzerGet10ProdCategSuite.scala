package com.tetraoxygen.reporting

import org.scalatest.{FunSuite, Matchers, BeforeAndAfter}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

class ProductsAnalyzerGet10ProdCategSuite extends FunSuite with Matchers with BeforeAndAfter {
  var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  test("getTop10ProductByCategory") {
    val rawData = Seq(
      Row("A1", 12.21, "2019-09-10 12:12:12", "C1", "10.7.7.7"),
      Row("A1", 12.21, "2019-09-10 12:12:12", "C1", "10.7.7.7"),
      Row("A2", 20.21, "2019-09-10 12:12:12", "C1", "10.7.7.7"),
      Row("X1", 13.21, "2019-09-10 12:12:12", "C2", "10.7.7.4"),
    )

    val salesMeta = new SaleStorageMeta(null)
    val schema = salesMeta.getDataSchema()

    val rowsRdd: RDD[Row] = spark.sparkContext.parallelize(rawData)
    val df = spark.createDataFrame(rowsRdd, schema)

    val result = ProductsAnalyzer.getTop10ProductByCategory(df).collect()
    assert(result.size === 3)

    val mappedResult = result.map(r => ((r.getString(0), r.getString(1)), (r.getLong(2), r.getInt(3)))).toMap
    assert(mappedResult(("C1", "A1")) === (2, 1))
    assert(mappedResult(("C1", "A2")) === (1, 2))
    assert(mappedResult(("C2", "X1")) === (1, 1))

  }

  after {
    spark.stop()
  }
}