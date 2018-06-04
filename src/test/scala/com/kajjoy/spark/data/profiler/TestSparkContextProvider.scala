package com.kajjoy.spark.data.profiler

import org.apache.spark.sql.SparkSession

trait TestSparkContextProvider {

  def getTestSparkContext(): SparkSession = {
    SparkSession
      .builder()
        .appName("spark-data-profiler")
        .master("local[*]")
      .getOrCreate()
  }

}
