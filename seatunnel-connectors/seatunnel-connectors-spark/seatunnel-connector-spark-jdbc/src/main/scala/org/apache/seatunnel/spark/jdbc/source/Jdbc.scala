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
package org.apache.seatunnel.spark.jdbc.source

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.util.TablesNamesFinder

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}
import org.apache.seatunnel.common.config.{CheckResult, TypesafeConfigUtils}
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.seatunnel.spark.jdbc.source.util.HiveDialect
import org.apache.spark.sql.execution.datasources.jdbc2.{JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, Row, SparkSession}

class Jdbc extends SparkBatchSource {

  var sourceTables: Array[String] = _

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    val dataFrame = jdbcReader(env.getSparkSession, config.getString("driver")).load()
    fillFieldMetadata(dataFrame)
  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config, "url", "table", "user", "password")
  }

  def jdbcReader(sparkSession: SparkSession, driver: String): DataFrameReader = {

    val reader = sparkSession.read
      .format("jdbc")
      .option("url", config.getString("url"))
      .option("dbtable", genDbTable)
      .option("user", config.getString("user"))
      .option("password", config.getString("password"))
      .option("driver", driver)

    Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "jdbc.", false)) match {

      case Success(options) =>
        val optionMap = options
          .entrySet()
          .foldRight(Map[String, String]())((entry, m) => {
            m + (entry.getKey -> entry.getValue.unwrapped().toString)
          })

        reader.options(optionMap)
      case Failure(_) => // do nothing
    }

    if (config.getString("url").startsWith("jdbc:hive2")) {
      JdbcDialects.registerDialect(new HiveDialect)
    }

    reader
  }

  private def genDbTable = {
    val table = config.getString("table")
    val lowerTable = table.toLowerCase
    // 如果是select语句，则拼接成(sql) t,否则直接返回
    if (lowerTable.contains("select") && lowerTable.contains("from")) {
      // 解析sql语句，获取表名
      parseSourceTables(table)
      s"(${table}) t"
    } else {
      // 初始化sourceTables数组只有table一个元素
      sourceTables = Array(table)
      table
    }
  }

  private def parseSourceTables(sql: String) = {
    val statement = CCJSqlParserUtil.parse(sql)
    val tablesFinder = new TablesNamesFinder()
    val tables = tablesFinder.getTables(statement)
    val tableList = tables.toList
    sourceTables = tableList.toArray
  }

  private def fillFieldMetadata(dataFrame: DataFrame) = {
    // 获取源表字段注释map
    val fieldCommentMap = getFieldCommentMap(sourceTables)
    var df = dataFrame
    dataFrame.schema.fields.filter(field => fieldCommentMap.contains(field.name)).foreach(field => {
        df = df.withColumn(field.name, df.col(field.name).as(field.name, field.withComment(fieldCommentMap(field.name)).metadata))
      }
    )
    df
  }

  private def getFieldCommentMap(tables: Array[String]): Map[String, String] = {
    val conn = JdbcUtils.createConnectionFactory(config)()
    val metaData = conn.getMetaData
    try {
      tables.flatMap { table =>
        // 提取schemaName和tableName
        val tableName = table.split("\\.").last.replace("%", "\\%").replace("_", "\\_")
        val schemaName = if (table.split("\\.").length > 1) table.split("\\.")(0).replace("%", "\\%").replace("_", "\\_") else null
        val resultSet = metaData.getColumns(null, schemaName, tableName, null)
        val tableSchema = scala.collection.mutable.Map[String, String]()
        while (resultSet.next()) {
          val columnName = resultSet.getString("COLUMN_NAME")
          val columnComment = resultSet.getString("REMARKS")
          // 注释不为空则添加到map中
          if (columnComment != null && columnComment.nonEmpty) {
            tableSchema += (columnName -> columnComment)
          }
        }
        tableSchema
      }.toMap
    } finally {
      conn.close()
    }
  }

  override def getPluginName: String = "Jdbc"
}
