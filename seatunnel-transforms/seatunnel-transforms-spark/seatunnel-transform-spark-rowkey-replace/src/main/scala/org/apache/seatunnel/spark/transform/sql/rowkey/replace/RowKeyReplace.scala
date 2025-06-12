package org.apache.seatunnel.spark.transform.sql.rowkey.replace

import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Dataset, Row}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * 主键替换转换器 表1：字段1->表2：字段2 规则含义：把表1的字段1替换成表2的字段2，条件是字段1等于表2的主键
 *
 * 1.主键替换转换器，如null，则全替换成默认值 1.1 基于表2的rowkey 表1：字段1->表2：字段2
 * 规则含义：把表1的字段1替换成表2的字段2，条件是字段1等于表2的主键 saleinfo:productlinecode=>productmae:name
 * 如果表1记录没有字段1，则打警告；如果包含字段1，且在表2中找到替换字段2，才替换。可理解为：如果能找到替换就替换
 *
 * Created by yp-tc-m-7161 on 16/11/23.
 */
class RowKeyReplace extends BaseSparkTransform {

  override def process(data: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val rules = config.getStringList("rules").asScala
    val columnOutputMap = scala.collection.mutable.LinkedHashMap[String, Column]()
    var result = data.alias("master")
    data.columns.foreach(column => {
      columnOutputMap.put(column.toUpperCase, col(s"master.${column}").as(column.toUpperCase))
    })
    var tableAliasSuffix = 0;
    for (rule <- rules) {
      tableAliasSuffix += 1
      val primaryColumn = rule.split("=>").head.split(":").last
      val tableName = rule.split("=>").last.split(":").head
      val column = rule.split("=>").last.split(":").last
      val df = env.getSparkSession.sql(s"SELECT * FROM $tableName")
        .alias(s"t${tableAliasSuffix}")
      result = result.join(df, result(primaryColumn) === df(df.columns.head), "left")
      columnOutputMap.put(primaryColumn.toUpperCase, col(s"t${tableAliasSuffix}.${column}").as(primaryColumn))
    }
    result = result.select(columnOutputMap.values.toList: _*)
    result.explain(true)
    result
  }

  /**
   * Return the plugin name, this is used in seatunnel conf DSL.
   *
   * @return plugin name.
   */
  override def getPluginName: String = "RowKeyReplace"
}
