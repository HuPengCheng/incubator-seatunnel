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

package org.apache.seatunnel.spark;

import org.apache.seatunnel.apis.base.api.BaseSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * a base interface indicates a sink plugin running on Spark.
 */
public abstract class BaseSparkSink<OUT> implements BaseSink<SparkEnvironment> {

    protected Config config = ConfigFactory.empty();

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    public abstract OUT output(Dataset<Row> data, SparkEnvironment env);

    /**
     * 删除老数据，保证任务幂等执行
     * @param env spark运行环境
     */
    public void cleanOldDataInSink(SparkEnvironment env) {
        // doThing
    }

    public Dataset<Row> cleanDataset(Dataset<Row> data, SparkEnvironment env) {
        if (config.hasPath(CONFIG_DROP_MODE) && 1 == config.getInt(CONFIG_DROP_MODE)) {
            return data.withColumn(FIELD_ETL_TASK_ID, config.hasPath(CONFIG_TASK_ID) ? functions.lit(config.getString(CONFIG_TASK_ID)) : functions.lit(null).cast("string"));
        }
        cleanOldDataInSink(env);
        return data;
    }
}
