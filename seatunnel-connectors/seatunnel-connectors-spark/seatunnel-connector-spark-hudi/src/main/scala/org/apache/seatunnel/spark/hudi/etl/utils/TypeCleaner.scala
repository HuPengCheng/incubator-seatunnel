package org.apache.seatunnel.spark.hudi.etl.utils

import org.apache.seatunnel.spark.hudi.etl.`type`.TypeParser
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, Decimal, DecimalType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Date
import java.util
import scala.collection.JavaConverters._


/**
 * Created by yp-tc-m-7161 on 16/11/11.
 */
object TypeCleaner {

    //hiveLconf/hudiLconf的字段类型需要修改
    //date/timestamp->String,bigdecimal/decimal->double
    def changeType(colNameType: util.LinkedHashMap[String, String]):util.LinkedHashMap[String, String] = {
       println("============before changeType==============")
        println(colNameType)
        val newColType = new util.LinkedHashMap[String, String]

        val colnumTypes = colNameType.asScala.map(col => {
            (col._1, col._2)
        })


        for((k,v) <- colnumTypes){
            var arr = v.split(",")
           var colType = arr(0)
            if(colType.equalsIgnoreCase("DATE")||colType.equalsIgnoreCase("TIMESTAMP")){
                arr(0) = "String"
            }else if(colType.equalsIgnoreCase("DECIMAL")||colType.equalsIgnoreCase("BIGDECIMAL")){
                arr(0) = "Double"
            }
            newColType.put(k,arr.mkString(","))
        }

        println("============before changeType==============")
        println(newColType)
        newColType
    }


    var typeList = List("string", "long", "integer", "timestamp", "date", "boolean", "bigdecimal", "double", "float","bigint");

    //hiveLconf/hudiLconf的字段类型需要修改
    //date/timestamp->String,bigdecimal/decimal->double
    def clean(sparkSession: SparkSession, dataframe: DataFrame): DataFrame = {
        registerUdf(sparkSession)
        val func = "cast_format"
        dataframe.selectExpr(dataframe.schema.fields.map(field => {
            field.dataType.typeName match  {
                case "date" | "timestamp"
                    => s"(${func}_${field.dataType.simpleString}(${field.name},'string', true) ) AS ${field.name}"
                case "decimal" => s"(${func}_${field.dataType.simpleString}(${field.name},'double', true) ) AS ${field.name}"
                case _ => field.name
            }
        }): _*)
    }

    /*
    {
            match field.dataType.typeName {
                case DateType.typeName => s"(${func}_${field.dataType.simpleString}(${field.name},'string', true) ) AS ${field.name}"
                case _ => field.name
            }
        }
     */
    def clean(sparkSession: SparkSession, colnameTypes: java.util.LinkedHashMap[String, String], dataframe: DataFrame, isFillDefault: Boolean = false): DataFrame = {

        cleanSql(sparkSession, colnameTypes, dataframe, isFillDefault)
    }


    def cleanSql(sparkSession: SparkSession, colnameTypes: java.util.LinkedHashMap[String, String], dataframe: DataFrame, isFillDefault: Boolean): DataFrame = {


        val castFunName = "cast_format"
        val tablename = "cast_format_" + System.currentTimeMillis()
        val sqlInfo = genCleanSql(sparkSession, colnameTypes, dataframe, castFunName, tablename, isFillDefault)

        registerUdf(sparkSession)

        dataframe.createOrReplaceTempView(tablename)

        println("========cleanSql==================")
        println(s"sql : ${sqlInfo._1}")
        println(s"isFillDefault : ${isFillDefault}")
        println("========cleanSql end==================")
        var df = sparkSession.sql(sqlInfo._1)
        sqlInfo._2.foreach(f => {
            df = df.withColumn(f._1, lit(null))
        })

        println(s"==============${sqlInfo._2.size} +++++${sqlInfo._2.mkString(",")}")

        df.printSchema()
        df
    }


    private def registerUdf(sparkSession: SparkSession) = {

        val cast_format_sequence = (value: Any, typeInfo: String, isFillDefault: Boolean) => {
            val parser: TypeParser = new TypeParser()
            parser.parseString(getDataType(typeInfo, isFillDefault), getDefaultValue(value, typeInfo, isFillDefault))

        }
        sparkSession.udf.register("cast_format_sequence", cast_format_sequence)

        val cast_format_string = (value: Any, typeInfo: String, isFillDefault: Boolean) => {
            val parser: TypeParser = new TypeParser()
            parser.parseString(getDataType(typeInfo, isFillDefault), getDefaultValue(value, typeInfo, isFillDefault))

        }
        sparkSession.udf.register("cast_format_string", cast_format_string)

        val cast_format_long = (value: Any, typeInfo: String, isFillDefault: Boolean) => {
            val parser: TypeParser = new TypeParser()
            parser.parseLong(getDataType(typeInfo, isFillDefault), getDefaultValue(value, typeInfo, isFillDefault))
        }
        sparkSession.udf.register("cast_format_long", cast_format_long)

        val cast_format_bigint = (value: Any, typeInfo: String, isFillDefault: Boolean) => {
            val parser: TypeParser = new TypeParser()
            parser.parseLong(getDataType(typeInfo, isFillDefault), getDefaultValue(value, typeInfo, isFillDefault))
        }
        sparkSession.udf.register("cast_format_bigint", cast_format_bigint)

        val cast_format_integer = (value: Any, typeInfo: String, isFillDefault: Boolean) => {
            val parser: TypeParser = new TypeParser()
            parser.parseInteger(getDataType(typeInfo, isFillDefault), getDefaultValue(value, typeInfo, isFillDefault))
        }
        sparkSession.udf.register("cast_format_integer", cast_format_integer)


        val cast_format_timestamp = (value: Any, typeInfo: String, isFillDefault: Boolean) => {
            val parser: TypeParser = new TypeParser()
            parser.parseTimestamp(getDataType(typeInfo, isFillDefault), getDefaultValue(value, typeInfo, isFillDefault))
        }
        sparkSession.udf.register("cast_format_timestamp", cast_format_timestamp)

        val cast_format_date = (value: Any, typeInfo: String, isFillDefault: Boolean) => {
            val parser: TypeParser = new TypeParser()
            val tmpValue = parser.parseDate(getDataType(typeInfo, isFillDefault), getDefaultValue(value, typeInfo, isFillDefault))
            if (tmpValue != null) {
                new Date(tmpValue.getTime)
            } else {
                null
            }

        }
        sparkSession.udf.register("cast_format_date", cast_format_date)

        val cast_format_boolean = (value: Any, typeInfo: String, isFillDefault: Boolean) => {
            val parser: TypeParser = new TypeParser()
            parser.parseBoolean(getDataType(typeInfo, isFillDefault), getDefaultValue(value, typeInfo, isFillDefault))
        }
        sparkSession.udf.register("cast_format_boolean", cast_format_boolean)

        val cast_format_bigdecimal = (value: Any, typeInfo: String, isFillDefault: Boolean) => {
            val parser: TypeParser = new TypeParser()
            parser.parseBigdecimal(getDataType(typeInfo, isFillDefault), getDefaultValue(value, typeInfo, isFillDefault))
        }
        sparkSession.udf.register("cast_format_bigdecimal", cast_format_bigdecimal)

        val cast_format_double = (value: Any, typeInfo: String, isFillDefault: Boolean) => {
            val parser: TypeParser = new TypeParser()
            parser.parseDouble(getDataType(typeInfo, isFillDefault), getDefaultValue(value, typeInfo, isFillDefault))
        }
        sparkSession.udf.register("cast_format_double", cast_format_double)


        val cast_format_float = (value: Any, typeInfo: String, isFillDefault: Boolean) => {
            val parser: TypeParser = new TypeParser()
            parser.parseFloat(getDataType(typeInfo, isFillDefault), getDefaultValue(value, typeInfo, isFillDefault))
        }
        sparkSession.udf.register("cast_format_float", cast_format_float)

    }

    private def getDefaultValue(value: Any, rule: String, isFillDefault: Boolean) = {
        if (null == value && isFillDefault && rule.split(",").length > 1) {
            val typeInfo = rule.split(",")(0)
            val defalutValue = rule.split(",")(1)
            if (typeInfo.equalsIgnoreCase("timestamp") && defalutValue.equalsIgnoreCase("now")) {
                new java.sql.Date(System.currentTimeMillis)
            } else {
                defalutValue
            }
        } else {
            value
        }
    }

    private def getDataType(rule: String, isFillDefault: Boolean): String = {
        if (isFillDefault) {
            rule.split(",")(0)
        } else {
            rule
        }
    }

    private def genCleanSql(sparkSession: SparkSession, colnameTypes: java.util.LinkedHashMap[String, String], dataframe: DataFrame, func: String, tableName: String, isFillDefault: Boolean): (String, Map[String, String]) = {

        val colnumTypes = colnameTypes.asScala.map(col => {
            (col._1.trim.toUpperCase, col._2)
        })

        val castColnums = dataframe.schema.fieldNames.filter(f => colnumTypes.keySet.contains(f)).map(f => {
            val typeInfo = colnumTypes(f)
            val dataType = typeInfo.split(",")(0).toLowerCase
            if (!typeList.contains(dataType)) {
                s"(${func}_string($f,'$typeInfo',$isFillDefault) ) AS $f"
            } else {
                s"(${func}_$dataType($f,'$typeInfo',$isFillDefault) ) AS $f"
            }
        })

        val notExistColnum = colnumTypes.filterNot(f => {
            dataframe.schema.fieldNames.contains(f._1)
        }).toMap

        println(s"========= dataframe.schema.fieldNames : ${dataframe.schema.fieldNames.mkString(";")}")

        println(s"========= notExistColnum : ${notExistColnum.mkString(";")}")

        val sql = s"select ${castColnums.mkString(",")}  from $tableName"
        (sql, notExistColnum)
    }

}
