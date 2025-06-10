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
package org.apache.seatunnel.spark.jdbc.sink

import scala.collection.JavaConversions._
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.{CheckResult, TypesafeConfigUtils}
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.datasources.jdbc2.{JDBCSaveMode, JdbcOptionsInWrite, JdbcUtils}

import java.sql.SQLException
import scala.util.{Failure, Success, Try}

class Jdbc extends SparkBatchSink {

  val DELETE_BATCH_SIZE = 10000

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val saveMode = config.getString("saveMode")
    if ("update".equals(saveMode)) {
      data.write.format("org.apache.spark.sql.execution.datasources.jdbc2").options(
        Map(
          "saveMode" -> JDBCSaveMode.Update.toString,
          "driver" -> config.getString("driver"),
          "url" -> config.getString("url"),
          "user" -> config.getString("user"),
          "password" -> config.getString("password"),
          "dbtable" -> config.getString("dbTable"),
          "useSsl" -> config.getString("useSsl"),
          "isolationLevel" -> config.getString("isolationLevel"),
          "customUpdateStmt" -> config.getString(
            "customUpdateStmt"
          ), // Custom mysql duplicate key update statement when saveMode is update
          "duplicateIncs" -> config.getString("duplicateIncs"),
          "showSql" -> config.getString("showSql"))).save()
    } else {
      val prop = new java.util.Properties()
      prop.setProperty("driver", config.getString("driver"))
      prop.setProperty("user", config.getString("user"))
      prop.setProperty("password", config.getString("password"))
      val writer = data.write.mode(saveMode)
      Try(TypesafeConfigUtils.extractSubConfigThrowable(config, "jdbc.", false)) match {

        case Success(options) =>
          val optionMap = options
            .entrySet()
            .foldRight(Map[String, String]())((entry, m) => {
              m + (entry.getKey -> entry.getValue.unwrapped().toString)
            })

          writer.options(optionMap)
        case Failure(_) => // do nothing
      }
      writer.jdbc(config.getString("url"), config.getString("dbTable"), prop)
    }

  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config, "driver", "url", "dbTable", "user", "password")
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "saveMode" -> "error",
        "useSsl" -> "false",
        "showSql" -> "true",
        "isolationLevel" -> "READ_UNCOMMITTED",
        "customUpdateStmt" -> "",
        "duplicateIncs" -> ""))
    config = config.withFallback(defaultConfig)
  }

  override def cleanAllDataInSink(env: SparkEnvironment): Unit = {
    val sql = s"truncate table ${config.getString("dbTable")}"
    executeSql(sql)
  }

  override def cleanDataByEtlIdInSink(env: SparkEnvironment): Unit = {
    val conn = JdbcUtils.createConnectionFactory(config)()
    val stmt = conn.createStatement()
    try {
      val deleteSql = s"delete from ${config.getString("dbTable")} where ETLTASKID = '${config.getString("task.id")}' limit $DELETE_BATCH_SIZE"
      // 执行删除语句，直到没有数据为止
      var count = 1
      while (count > 0) {
        count = stmt.executeUpdate(deleteSql)
        if (count > 0) {
          println(s"${java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))} - Deleted $count rows from ${config.getString("dbTable")} where ETLTASKID = '${config.getString("task.id")}'")
        }
      }
      println(s"${java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))} - All data with ETLTASKID = '${config.getString("task.id")}' has been deleted from ${config.getString("dbTable")}")
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      stmt.close()
      conn.close()
    }
  }

  private def executeSql(sql: String) = {
    val conn = JdbcUtils.createConnectionFactory(config)()
    try {
      val stmt = conn.createStatement()
      try {
        println("execute sql: " + sql)
        stmt.execute(sql)
      } catch {
        case e: SQLException => e.printStackTrace()
      } finally {
        stmt.close()
      }
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      conn.close()
    }
  }

  override def getPluginName: String = "Jdbc"
}
