package com.tetraoxygen.reporting

import com.tetraoxygen.reporting.domain.RawGeoInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

class GeoAnalizerSuite extends FunSuite with BeforeAndAfter  {
  var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  test("count GetTop10CountriesByTotalSales") {
    val salesData = Seq(
      Row("A1", 1.01, "2019-09-10 12:12:12", "C1", "100.7.7.7"),
      Row("A1", 2.16, "2019-09-10 12:12:12", "C1", "150.7.7.7"),
      Row("A3", 4.04, "2019-09-10 12:12:12", "C1", "242.7.7.7"),
      Row("X2", 8.00, "2019-09-10 12:12:12", "C2", "251.7.7.4"),
    )

    val salesDF = buildDF(salesData, new SaleStorageMeta(null))

    val geoData = Seq(
      Row("100.7.7.1/8", "A"),
      Row("140.7.7.1/8", "B"),
      Row("150.7.7.1/8", "C"),
      Row("130.7.7.1/8", "D"),
      Row("240.0.0.1/4", "F"),
    )

    val geoDF = buildDF(geoData, new GeoStorageMeta(null))

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val geoDS = geoDF.as[RawGeoInfo]

    val result = GeoAnalyzer.getTop10CountriesByTotalSales(salesDF, geoDS).collect()
    assert(result.length === 3)

    val mappedResult: Map[String, Double] = result.map(r => (r.getString(0), r.getDouble(1))).toMap
    assert(mappedResult("F") === 12.04)
    assert(mappedResult("A") === 1.01)
    assert(mappedResult("C") === 2.16)
  }

  private def buildDF(data: Seq[Row], storageMeta: StorageMeta): DataFrame ={
    val schema: StructType = storageMeta.getDataSchema()
    val dataRDD: RDD[Row] = spark.sparkContext.parallelize(data)
    spark.createDataFrame(dataRDD, schema)
  }

  after {
    spark.stop()
  }
}
