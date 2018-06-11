package com.kajjoy.spark.data.profiler.profile

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

case class DataFrameProfile(df: DataFrame)  {

  df.cache

  lazy val spark = SparkSession.builder().getOrCreate()

  val columnProfiles  : List[ColumnProfile] =
    for (c <- getColumnNames(df))
      yield ColumnProfile.ColumnProfileFactory(df,c)

  private def getColumnNames(df: DataFrame) : List[String] = {
    var columnNames = new ListBuffer[String]()
    findFields("", df.schema, columnNames)
    columnNames.toList
  }

  private def findFields(path: String, dt: DataType, columnNames: ListBuffer[String]): Unit = dt match {
    case s: StructType =>
      s.fields.foreach(f => findFields(path + "." + f.name, f.dataType, columnNames))
    case s: ArrayType => findFields(path, s.elementType, columnNames)
    case other =>
      columnNames += path.substring(1)
  }


  val header : List[String] = List("Column Name","Record Count", "Unique Values", "Empty Strings" ,"Null Values", "Percent Fill", "Percent Numeric", "Max Length")

  def toDataFrame : DataFrame = {
    def dfFromListWithHeader(data: List[List[String]], header: String) : DataFrame = {
      val rows = data.map{x => Row(x:_*)}
      val rdd = spark.sparkContext.parallelize(rows)
      val schema = StructType(header.split(",").
        map(fieldName => StructField(fieldName, StringType, true)))
      spark.sqlContext.createDataFrame(rdd, schema)
    }
    val data = columnProfiles.map(_.columnData)
    dfFromListWithHeader(data,header.mkString(","))
  }

  override def toString : String = {
    val colummProfileStrings : List[String] = columnProfiles.map(_.toString)
    (header.mkString(",") :: columnProfiles).mkString("\n")
  }
}

