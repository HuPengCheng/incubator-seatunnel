package org.apache.seatunnel.spark.transform.union

import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable

class UnionAll extends BaseSparkTransform {

  override def process(data: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    // 可变数组存储字段全集
    val columns = mutable.LinkedHashSet[String]()  // 保持顺序去重
    val unionTable = config.getString("union_table")
    columns ++= data.columns
    val unionDf = env.getSparkSession.sql(s"SELECT * FROM $unionTable")
    val tableColumns = unionDf.schema.fieldNames
    columns ++= tableColumns
    selectFullColumns(data, columns).union(selectFullColumns(unionDf, columns))
  }

  private def selectFullColumns(data: Dataset[Row], columns: mutable.LinkedHashSet[String]) = {
    data.select(columns.map(column => {
      if (data.columns.contains(column)) {
        data.col(column)
      } else {
        lit(null).as(column)
      }
    }).toSeq: _*)
  }

  /**
   * Return the plugin name, this is used in seatunnel conf DSL.
   *
   * @return plugin name.
   */
  override def getPluginName: String = "union"
}
