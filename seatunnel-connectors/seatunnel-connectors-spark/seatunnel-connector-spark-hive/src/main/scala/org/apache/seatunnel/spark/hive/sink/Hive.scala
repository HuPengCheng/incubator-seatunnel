/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.spark.hive.sink

import java.util
import scala.collection.JavaConversions._
import org.apache.seatunnel.common.config.{CheckConfigUtil, CheckResult}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrameWriter, Dataset, Row}

class Hive extends SparkBatchSink with Logging {

  override def checkConfig(): CheckResult = {
    if (config.hasPath("sql")) {
      CheckResult.success()
    } else {
      CheckConfigUtil.checkAllExists(config, "result_table_name")
    }
  }

  override def output(df: Dataset[Row], environment: SparkEnvironment): Unit = {
    val sparkSession = df.sparkSession
    if (config.hasPath("sql")) {
      val sql = config.getString("sql")
      sparkSession.sql(sql)
    } else {
      val resultTableName = config.getString("result_table_name")
      var sinkFrame = if (config.hasPath("sink_columns")) {
        df.selectExpr(config.getString("sink_columns").split(","): _*)
      } else {
        df
      }
      var partitionList: util.List[String] = new util.ArrayList[String]()
      val partitionFields: util.List[String] = new util.ArrayList[String]()
      var staticValuePartition = false
      if (config.hasPath("partition_by")) {
        partitionList = config.getStringList("partition_by")
        partitionList.foreach(p => {
          val partitionRule = p.split("=")
          if (partitionRule.length == 2) {
            partitionFields.add(partitionRule(0))
            val value = if (partitionRule(1).indexOf("'") < partitionRule(1).lastIndexOf("'")) {
              staticValuePartition = true
              partitionRule(1).substring(partitionRule(1).indexOf("'") + 1, partitionRule(1).lastIndexOf("'"))
            } else if (partitionRule(1).indexOf("\"") < partitionRule(1).lastIndexOf("\"")) {
              staticValuePartition = true
              partitionRule(1).substring(partitionRule(1).indexOf("\"") + 1, partitionRule(1).lastIndexOf("\""))
            } else {
              partitionRule(1)
            }
            sinkFrame = sinkFrame.withColumn(partitionRule(0), if (staticValuePartition) lit(value) else sinkFrame.col(value))
          } else {
            partitionFields.add(partitionRule(0))
          }
        })
      }
      val tableExists = environment.getSparkSession.catalog.tableExists(resultTableName)
      val frameWriter: DataFrameWriter[Row] =
      if (tableExists) {
        // 获取hive字段列表作为map
        val columns = environment.getSparkSession.catalog.listColumns(resultTableName).collect()
        val sourceColumns = sinkFrame.schema.fields.map(f => f.name.toLowerCase).toSet
        // columns中存在的字段直接使用，不存在的字段赋值null
        sinkFrame.selectExpr(columns.map(c => if (sourceColumns.contains(c.name.toLowerCase)) c.name else s"null as ${c.name}"): _*)
          .write.format("Hive")
          .mode("append")
      } else {
        sinkFrame.write.format("Hive").mode("overwrite")
      }
      if (config.hasPath("save_mode") && "overwrite".equalsIgnoreCase(config.getString("save_mode")) && tableExists) {
        environment.getSparkSession.sql(s"truncate table ${resultTableName}")
      }
      frameWriter.format(if (config.hasPath("format")) config.getString("format") else "hive")
      if (config.hasPath("partition_by")) {
        if(tableExists && staticValuePartition) environment.getSparkSession.sql(s"alter table ${resultTableName} drop if exists partition(${partitionList.head})")
        frameWriter.partitionBy(partitionFields: _*).saveAsTable(resultTableName)
      } else {
        frameWriter.saveAsTable(resultTableName)
      }
      if (!tableExists && config.hasPath("comment")) {
        // 表注释
        val comment = config.getString("comment")
        // ALTER TABLE 表名 SET TBLPROPERTIES ('comment' = '注释内容');
        environment.getSparkSession.sql(s"ALTER TABLE ${resultTableName} SET TBLPROPERTIES ('comment' = '$comment')")
      }
    }
  }

  override def getPluginName: String = "Hive"
}
