package org.apache.seatunnel.spark.transform.sql.column.replace

import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class ColumnReplace extends BaseSparkTransform {

  override def process(data: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val sparkSession = env.getSparkSession
    // rule = 1edabicustomerdomaincopy:CUSTOMERNUMBER=>3edabicustomerdomaincopy:FIRST_TRX_DATE,LAST_TRX_DATE
    val rules = config.getStringList("rules").asScala
    val dfMap = scala.collection.mutable.Map[String, Dataset[Row]]()
    val columnDfMap = scala.collection.mutable.Map[String, Dataset[Row]]()
    var result = data
    rules.foreach(rule => {
      val primaryColumn = rule.split("=>").head.split(":").last
      val tableName = rule.split("=>").last.split(":").head
      val columns = rule.split("=>").last.split(":").last.split(",")
      val df = sparkSession.sql(s"select * from ${tableName}")
      result = result.join(df, result(primaryColumn) === df(df.columns.head), "left")
      dfMap.put(tableName, df)
      columns.foreach(column => {
        columnDfMap.put(column, df)
      })
    })
    result.select(data.columns.map(column => {
      if (!columnDfMap.contains(column)) {
        result(column)
      } else {
        columnDfMap(column)(column)
      }
    }): _*)
  }


  /**
   * Return the plugin name, this is used in seatunnel conf DSL.
   *
   * @return plugin name.
   */
  override def getPluginName: String = "ColumnReplace"
}
