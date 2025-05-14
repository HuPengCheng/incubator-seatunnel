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
            return dataframe
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
        TypeCleaner.registerUdf(sparkSession)
        val newColnums = dataframe.schema.fields.map(field => {
            val targetFieldName = field.name.toUpperCase
            val fieldType = field.dataType.typeName.toLowerCase
            if (fieldType.equals("date") || fieldType.equals("timestamp")) {
                s"(cast_format_string(${field.name},'${field.dataType.simpleString}', true) ) AS $targetFieldName"
            } else if (fieldType.startsWith("decimal")) {
                s"(cast_format_double(${field.name},'decimal', true) ) AS $targetFieldName"
            } else if (fieldType.equals("string")) {
                s"${func}(${field.name}) AS $targetFieldName"
            } else {
                s"${field.name} AS $targetFieldName"
            }
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
