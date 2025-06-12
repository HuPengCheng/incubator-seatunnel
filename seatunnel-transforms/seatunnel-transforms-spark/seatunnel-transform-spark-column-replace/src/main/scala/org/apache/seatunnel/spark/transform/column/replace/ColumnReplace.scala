package org.apache.seatunnel.spark.transform.column.replace

import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Dataset, Row}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class ColumnReplace extends BaseSparkTransform {

  override def process(data: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val sparkSession = env.getSparkSession
    // rule = 1edabicustomerdomaincopy:CUSTOMERNUMBER=>3edabicustomerdomaincopy:FIRST_TRX_DATE,LAST_TRX_DATE
    val rules = config.getStringList("rules").asScala
    val dfMap = scala.collection.mutable.Map[String, Dataset[Row]]()
    var result = data.alias("master")
    // 有序map存储字段名和输出的column的关系
    val columnOutputMap = scala.collection.mutable.LinkedHashMap[String, Column]()
    data.columns.foreach(column => {
      columnOutputMap.put(column.toUpperCase, col(f"master.${column}").as(column.toUpperCase))
    })
    var tableAliasSuffix = 0;
    for (rule <- rules) {
      tableAliasSuffix += 1
      val primaryColumn = rule.split("=>").head.split(":").last
      val tableName = rule.split("=>").last.split(":").head
      val columns = rule.split("=>").last.split(":").last.split(",")
      val df = sparkSession.sql(s"select * from ${tableName}")
        .alias(f"t${tableAliasSuffix}")
      result = result.join(df, result(primaryColumn) === df(df.columns.head), "left")
      dfMap.put(tableName, df)
      columns.foreach(column => {
        columnOutputMap.put(column.toUpperCase, col(f"t${tableAliasSuffix}.${column}").as(column.toUpperCase))
      })
    }
    columnOutputMap.values.toList
    result = result.select(columnOutputMap.values.toList: _*)
    result.explain(true)
    result
  }


  /**
   * Return the plugin name, this is used in seatunnel conf DSL.
   *
   * @return plugin name.
   */
  override def getPluginName: String = "ColumnReplace"
}
