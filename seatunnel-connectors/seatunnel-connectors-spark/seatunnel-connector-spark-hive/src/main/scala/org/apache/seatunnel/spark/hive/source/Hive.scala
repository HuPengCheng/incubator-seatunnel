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
package org.apache.seatunnel.spark.hive.source

import com.alibaba.druid.sql.ast.SQLStatement
import com.alibaba.druid.sql.dialect.hive.parser.HiveStatementParser
import com.alibaba.druid.sql.dialect.hive.visitor.HiveSchemaStatVisitor
import com.alibaba.druid.stat.TableStat
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.spark.sql.{Dataset, Row}

import java.util

class Hive extends SparkBatchSource {

  override def checkConfig(): CheckResult = {
    checkAllExists(config, "pre_sql")
  }

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    env.getSparkSession.sql(config.getString("pre_sql"))
  }

  override def getPluginName: String = "Hive"

  /**
   * This is a lifecycle method, this method will be executed after Plugin created.
   *
   * @param env environment
   */
  override def prepare(env: SparkEnvironment): Unit = {
    val parser = new HiveStatementParser(config.getString("pre_sql"))
    val sqlStatement: SQLStatement = parser.parseStatement
    val visitor: HiveSchemaStatVisitor = new HiveSchemaStatVisitor
    sqlStatement.accept(visitor)
    val tables: util.Map[TableStat.Name, TableStat] = visitor.getTables
    import scala.collection.JavaConversions._
    for (t <- tables.keySet) {
      // 强制刷新hive元数据，防止表结构变更导致找不到字段
      println(s"refresh table $t")
      env.getSparkSession.catalog.refreshTable(t.getName)
    }
  }
}
