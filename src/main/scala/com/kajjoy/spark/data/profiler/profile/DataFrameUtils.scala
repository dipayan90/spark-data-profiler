package com.kajjoy.spark.data.profiler.profile

import org.apache.spark.sql._

object DataFrameUtils {
  implicit class DataFrameImprovements(df: org.apache.spark.sql.DataFrame) {
    def profile : DataFrame = {
      DataFrameProfile(df).toDataFrame
    }
  }
}

