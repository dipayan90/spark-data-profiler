package com.kajjoy.spark.data.profiler.profile

import com.kajjoy.spark.data.profiler.TestSparkContextProvider
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSpec

class ColumnProfileSpec extends FunSpec with TestSparkContextProvider  {

  val spark : SparkSession = getTestSparkContext()
  import spark.implicits._

  describe("ColumnProfile"){

    it ("constructs a Column profile objects with expected statistcics for a single column"){
      val colProfile  = ColumnProfile.ColumnProfileFactory(baseballDf, "city")
      assert(colProfile.columnName === "city")
      assert(colProfile.totalDataSetSize === 5)
      assert(colProfile.uniqueValues === 3)
      assert(colProfile.nullValues === 0)

      val colProfile2 = ColumnProfile.ColumnProfileFactory(baseballDf, "lastChampionship")
      assert(colProfile2.nullValues ===0 )
      assert(colProfile2.emptyStringValues === 1)
      assert(colProfile2.numericValues === 4)

    }
  }

  def baseballDf: DataFrame = {
    Seq(
      ("Mets", "1986", "New York"),
      ("Yankees", "2009", "New York"),
      ("Cubs", "2016", "Chicago"),
      ("Cubs", "2005", "Chicago"),
      ("Nationals", "", "Washington")
    ).toDF("team", "lastChampionship", "city")
  }
}