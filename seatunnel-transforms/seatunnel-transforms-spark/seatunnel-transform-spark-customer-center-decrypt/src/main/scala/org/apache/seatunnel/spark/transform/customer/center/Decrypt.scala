package org.apache.seatunnel.spark.transform.customer.center

import org.apache.commons.lang3.StringUtils
import org.apache.seatunnel.spark.encrypt.AES
import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, Row}

class Decrypt extends BaseSparkTransform {

  override def process(data: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val sparkSession = env.getSparkSession
    val KEY = "I am a fool, OK?"

    val needDecryptColumns = config.getString("rule").split(",").map(_.toUpperCase).toList
    val fields = data.schema.fieldNames.map(_.toUpperCase).toList
    println("输出的字段：" + fields)

    val decryptUdf = udf((str: String) => {
      if (!StringUtils.isBlank(str)) AES.decryptFromBase64(str, KEY)
      else str
    })

    val finalFields = fields.map(field =>
      if (needDecryptColumns.contains(field)) decryptUdf(data.col(field)).as(field)
      else data.col(field)
    )

    data.select(finalFields: _*)
  }

  override def getPluginName: String = "decrypt"
}