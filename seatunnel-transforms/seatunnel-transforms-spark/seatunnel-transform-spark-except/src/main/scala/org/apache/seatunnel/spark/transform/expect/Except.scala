package org.apache.seatunnel.spark.transform.expect

import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.{Dataset, Row}

class Except extends BaseSparkTransform {

  override def process(data: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val expectTable = config.getString("expect_table")
    val sparkSession = env.getSparkSession
    val expectDF = sparkSession.sql(s"SELECT * FROM $expectTable")
    val primaryField1 = data.schema.fieldNames.head
    val expectField1 = expectDF.schema.fieldNames.head
    // 如果两张表都有idnouse或idnose字段，则直接返回data
    if (hasIdNoUse(data) && hasIdNoUse(expectDF)) {
      return data
    }
    // select * from primaryDF left join expectDF on primaryDF.field1 = expectDF.field1 where expectDF.field1 is null
    data.join(expectDF, data(primaryField1) === expectDF(expectField1), "left")
      .filter(expectDF(expectField1).isNull)
      .select(data.columns.map(col => data(col)): _*)
  }

  private def hasIdNoUse(data: Dataset[Row]) = {
    data.schema.fieldNames.exists(field => field.equalsIgnoreCase("idnouse") || field.equalsIgnoreCase("idnose"))
  }

  /**
   * Return the plugin name, this is used in seatunnel conf DSL.
   *
   * @return plugin name.
   */
  override def getPluginName: String = "expect"
}
