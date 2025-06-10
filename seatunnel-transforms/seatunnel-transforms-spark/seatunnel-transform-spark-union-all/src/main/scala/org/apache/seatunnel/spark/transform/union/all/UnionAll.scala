package org.apache.seatunnel.spark.transform.union.all

import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable

class UnionAll extends BaseSparkTransform {

  override def process(data: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    // 可变数组存储字段全集
    val columns = mutable.LinkedHashSet[String]()  // 保持顺序去重
    val tables = config.getStringList("tables").asScala
    // 遍历所有表, 生成字段全集，如果存在则不添加
    columns ++= data.columns
    tables.foreach(table => {
      val tableDF = env.getSparkSession.sql(s"SELECT * FROM $table")
      val tableColumns = tableDF.schema.fieldNames
      columns ++= tableColumns
    })
    var resultDF = selectFullColumns(data, columns)
    // 遍历所有表，进行union
    tables.foreach(table => {
      resultDF.union(selectFullColumns(env.getSparkSession.sql(s"SELECT * FROM $table"), columns))
    })
    // 返回结果
    resultDF
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
  override def getPluginName: String = "UnionAll"
}
