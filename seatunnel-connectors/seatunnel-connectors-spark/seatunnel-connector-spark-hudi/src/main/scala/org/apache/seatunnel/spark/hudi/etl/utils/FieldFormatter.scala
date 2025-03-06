package org.apache.seatunnel.spark.hudi.etl.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util
import java.util.regex.Pattern

/**
 * Created by Paul-1 on 2019/1/29.
 */
object FieldFormatter {

    private val REPLACE_PATTERN = Pattern.compile("\\n|\\r|\01")

    def strDropDelims(sparkSession: SparkSession, dataframe: DataFrame, needDropDelims: Boolean = false): DataFrame = {

        dropDelimsSql(sparkSession, dataframe, needDropDelims)
    }

    def dropDelimsSql(sparkSession: SparkSession, dataframe: DataFrame, needDropDelims: Boolean): DataFrame = {
        if (!needDropDelims) {
            dataframe
        }
        val clearFunName = "drop_import_delims"
        val tablename = "drop_import_delims_" + System.currentTimeMillis()
        dataframe.createGlobalTempView(tablename)
        val newName = "global_temp." + tablename;
        val sql = getDropSql(sparkSession, dataframe, clearFunName, newName)
        registerUdf(sparkSession)
        println("========clearHiveSql start==================")
        println(s"sql : $sql")
        var finalDf = sparkSession.sql(sql)

        println("========clearHiveSql end==================")
        finalDf
    }

    def registerUdf(sparkSession: SparkSession) = {

        val drop_import_delims = (value: Any) => {
            if (value != null) {
                REPLACE_PATTERN.matcher(value.toString).replaceAll("")
            } else {
                null
            }
        }
        sparkSession.udf.register("drop_import_delims", drop_import_delims)
    }

    private def getDropSql(sparkSession: SparkSession, dataframe: DataFrame, func: String, tableName: String): String = {

        val newColnums = dataframe.schema.fieldNames.map(col => {
            s"${func}($col) AS $col"
        })
        /*val newColnums = colnameTypes.asScala.map(col => {
            val colnum = col._1
            //            if(col._2.split(",").length > 3 && col._2.split(",")(3).toUpperCase.equals("CLEAN")){
            s"${func}($colnum) AS $colnum"
            //            }else{
            //                s"$colnum AS $colnum"
            //            }
        })*/

        val sql = s"select ${newColnums.mkString(",")}  from $tableName"
        sql
    }


}
