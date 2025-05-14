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
package org.apache.seatunnel.spark.hudi.sink

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.DataSourceWriteOptions.{HIVE_DATABASE_OPT_KEY, HIVE_PARTITION_FIELDS_OPT_KEY, HIVE_PASS_OPT_KEY, HIVE_SYNC_ENABLED_OPT_KEY, HIVE_TABLE_OPT_KEY, HIVE_URL_OPT_KEY, HIVE_USER_OPT_KEY, KEYGENERATOR_CLASS_OPT_KEY, OPERATION_OPT_KEY, PARTITIONPATH_FIELD_OPT_KEY, PAYLOAD_CLASS_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.hudi.Config.{DEFAULT_SAVE_MODE, DROP_MODE, HOODIE_BASE_PATH, HOODIE_TABLE_NAME, IS_ENCRYPT, NEED_DROP_DELIMS, SAVE_MODE, TASK_ID}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.seatunnel.spark.hudi.etl.utils.{FieldFormatter, TypeCleaner}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Row}

import java.net.URI
import java.time.Instant
import java.util
import scala.collection.JavaConversions._

class Hudi extends SparkBatchSink {

  private var tableExists = false

  override def getPluginName: String = "Hudi"
  val defaultBasePath = "/user/ypdata/hudi"
  val ETL_TASK_ID = "ETLTASKID"

  override def checkConfig(): CheckResult = {
    checkAllExists(config, HOODIE_TABLE_NAME)
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        SAVE_MODE -> DEFAULT_SAVE_MODE))
    config = config.withFallback(defaultConfig)
  }

  /**
   * 删除老数据，保证任务幂等执行
   *
   * @param env spark运行环境
   */
  override def cleanOldDataInSink(env: SparkEnvironment): Unit = {
    super.cleanOldDataInSink(env)
    val basePath = getBasePath
    val hdfs = getHdfs(basePath)
    tableExists = hdfs.exists(new Path(basePath))
    if (tableExists && config.hasPath(DROP_MODE)) {
      val hudiTableDf = env.getSparkSession.read.format("hudi").load(basePath + "/*/*/*")
      if ("1".equals(config.getString(DROP_MODE))) {
        if (hudiTableDf.schema.fieldNames.contains(ETL_TASK_ID)) {
          val needDeleteDf = hudiTableDf.filter(s"$ETL_TASK_ID = '${config.getString(TASK_ID)}'")
          deleteOldDataIfNecessaryByHudi(needDeleteDf, basePath)
          println("Delete successful by ETLTASKID")
        } else {
          println("Can't find  ETLTASKID")
        }
      } else if ("0".equals(config.getString(DROP_MODE))) {
        deleteOldDataIfNecessaryByHudi(hudiTableDf, basePath)
        println("Delete All")
      } else {
        println("Dont delete!!!")
      }
    } else {
      println(basePath + "唯恐")
    }
  }

  private def getBasePath: String = {
    val tablePath = config.getString(HOODIE_TABLE_NAME).split("\\.")
    var basePath = if (config.hasPath(HOODIE_BASE_PATH)) config.getString(HOODIE_BASE_PATH) else defaultBasePath
    if (basePath.endsWith("/")) {
      basePath = basePath.substring(0, basePath.length - 1)
    }
    basePath + "/" + tablePath(0) + "/" + tablePath(1)
  }

  override def output(df: Dataset[Row], environment: SparkEnvironment): Unit = {
    var dataframe = df.toDF()
    dataframe = cleanDf(dataframe, environment)
    dataframe.explain(true)
    val writer = dataframe.write.format("hudi")
    fillProperties(writer)
    writer.mode(config.getString(SAVE_MODE))
      .save(getBasePath)
  }

  private def cleanDf(dataframe: DataFrame, environment: SparkEnvironment): DataFrame = {
    println("========cleanDf start==================")
    dataframe.printSchema()
//    val needDropDelims = if (config.hasPath(NEED_DROP_DELIMS)) {
//      config.getBoolean(NEED_DROP_DELIMS)
//    } else {
//      false
//    }
//    val isEncrypt = if (config.hasPath(IS_ENCRYPT)) {
//      config.getBoolean(IS_ENCRYPT)
//    } else {
//      false
//    }
    // 清理表中`\t|\n|\r|\01`等特殊字符
    val dropDf = FieldFormatter.strDropDelims(environment.getSparkSession, dataframe, true)
//    result = Encryptor.encrypt(environment.getSparkSession, result, isEncrypt)
    println("========cleanDf end==================")
    dropDf.printSchema()
    dropDf
  }

  private def fillProperties(writer: DataFrameWriter[Row]): Unit = {
    var basePath = defaultBasePath
    val hudiProps = getHudiProp
    hudiProps.putAll(getHiveSyncProp)
    for (e <- config.entrySet()) {
      // 页面输入base path
      if (!HOODIE_BASE_PATH.equals(e.getKey)) {
        writer.option(e.getKey, String.valueOf(e.getValue.unwrapped()))
      } else {
        basePath = String.valueOf(e.getValue.unwrapped())
        if (basePath.endsWith("/")) {
          basePath = basePath.substring(0, basePath.length - 1)
        }
      }
    }

    // 处理base path
    val pathSuffix = hudiProps.get(HIVE_DATABASE_OPT_KEY) + "/" + hudiProps.get(HIVE_TABLE_OPT_KEY)
    if (!basePath.endsWith(pathSuffix)) {
      basePath = basePath + "/" + pathSuffix
    }

    writer.options(hudiProps)
  }

  private def getHiveSyncProp: util.HashMap[String, String] = {
    val tablePath = config.getString(HOODIE_TABLE_NAME).split("\\.")
    val schemaName = tablePath(0)
    val tableName = tablePath(1)
    var hiveSyncProperties = new util.HashMap[String, String]
    hiveSyncProperties.put(HIVE_SYNC_ENABLED_OPT_KEY, "true")
    hiveSyncProperties.put(HIVE_DATABASE_OPT_KEY, schemaName)
    hiveSyncProperties.put(HIVE_TABLE_OPT_KEY, tableName)
    hiveSyncProperties.put(HIVE_USER_OPT_KEY, "hive")
    hiveSyncProperties.put(HIVE_PASS_OPT_KEY, "hive")
    if (!config.hasPath(HIVE_URL_OPT_KEY)) {
      hiveSyncProperties.put(HIVE_URL_OPT_KEY, "jdbc:hive2://data71:10000/default;principal=hive/_HOST@HADOOP.COM")
    }
    hiveSyncProperties.put(HIVE_PARTITION_FIELDS_OPT_KEY, "dt")
    hiveSyncProperties
  }

  private def getHudiProp: util.HashMap[String, String] = {
    var hudiProperties = new util.HashMap[String, String]
    hudiProperties.put("hoodie.datasource.write.precombine.field", config.getString(PARTITIONPATH_FIELD_OPT_KEY))
    hudiProperties.put("hoodie.deltastreamer.keygen.timebased.timestamp.type", "DATE_STRING")
    hudiProperties.put("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd")
    hudiProperties.put("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyy/MM/dd")
    hudiProperties.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.utilities.keygen.TimestampBasedKeyGenerator")
    hudiProperties.put("hoodie.insert.shuffle.parallelism", "10")
    hudiProperties.put("hoodie.upsert.shuffle.parallelism", "10")
    hudiProperties
  }

  def getHdfs(path: String): FileSystem = {
    val conf = new Configuration()
    FileSystem.newInstance(URI.create(path), conf)
  }

  def deleteOldDataIfNecessaryByHudi(needDeleteDf: DataFrame, basePath: String): Unit = {
    val hudiProperties = getHudiProp
    hudiProperties.put(OPERATION_OPT_KEY, "delete")
    hudiProperties.put(PAYLOAD_CLASS_OPT_KEY, "org.apache.hudi.common.model.EmptyHoodieRecordPayload")
    for (e <- config.entrySet()) {
      // 屏蔽页面输入base path
      if (!HOODIE_BASE_PATH.equals(e.getKey)) {
        hudiProperties.put(e.getKey, String.valueOf(e.getValue.unwrapped()))
      }
    }

    //hudiProperties.put("hoodie.bloom.index.prune.by.ranges","false")

    needDeleteDf.write.format("hudi").
      options(hudiProperties).
      mode(Append).
      save(basePath)
    println("delete success!!!")
  }
}
