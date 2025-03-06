package org.apache.seatunnel.spark.encrypt

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
 *
 */
object Encryptor {

    var typeList = List("aescryptencrypt", "sha256encrypt", "tuomin", "md5encrypt", "md5md5encrypt", "sm3encrypt");

    val KEY = "3b8232ddf5354545a94a5d6b3263f7c3"

    def encrypt(sparkSession: SparkSession, colnameTypes: java.util.LinkedHashMap[String, String], dataframe: DataFrame, isneedEncrypted: Boolean): DataFrame = {
        if (isneedEncrypted) {
            encryptSql(sparkSession, colnameTypes, dataframe)
        } else {
            dataframe
        }
    }


    def encryptSql(sparkSession: SparkSession, colnameTypes: java.util.LinkedHashMap[String, String], dataframe: DataFrame): DataFrame = {

        val castFunName = "encrypt"
        val tablename = "encrypt_" + System.currentTimeMillis()
        val sql = genEncryptSql(colnameTypes, castFunName, tablename)

        registerUdf(sparkSession)

        dataframe.createOrReplaceTempView(tablename)

        println("========encryptsql==================")
        var df = sparkSession.sql(sql)

        df.printSchema()
        df
    }


    def registerUdf(sparkSession: SparkSession) = {

        val encrypt_aescryptencrypt = (value: Any) => {
            AEScrypt.encryptAES(value.toString, KEY)
        }
        sparkSession.udf.register("encrypt_aescryptencrypt", encrypt_aescryptencrypt)

        val encrypt_sha256encrypt = (value: Any) => {
            SHA256.SHA256Encrypt(value.toString)
        }
        sparkSession.udf.register("encrypt_sha256encrypt", encrypt_sha256encrypt)

        val encrypt_md5encrypt = (value: Any) => {
            MD5.md5x(value.toString)
        }
        sparkSession.udf.register("encrypt_md5encrypt", encrypt_md5encrypt)

        val encrypt_tuomin = (value: Any) => {
            tuomin(value.toString)
        }
        sparkSession.udf.register("encrypt_tuomin", encrypt_tuomin)

        //20230725 add md5加密
        val encrypt_md5md5encrypt = (value: Any) => {
            MD5.md5(value.toString)
        }
        sparkSession.udf.register("encrypt_md5md5encrypt", encrypt_md5md5encrypt)
        //20230725 add end

        //20230725 add sm3加密
        val encrypt_sm3encrypt = (value: Any) => {
            SM3.encrypt(value.toString)
        }
        sparkSession.udf.register("encrypt_sm3encrypt", encrypt_sm3encrypt)
        //20230725 add end
    }

    protected def aescryptEncrypt(str: String): String = AEScrypt.encryptAES(str, KEY)

    protected def sha256Encrypt(str: String): String = SHA256.SHA256Encrypt(str)

    protected def md5Encrypt(str: String): String = MD5.md5x(str)

    //20230725 add md5加密
    protected def md5md5Encrypt(str: String): String = MD5.md5(str)
    //20230725 add end

    //20230725 add sm3加密
    protected def sm3Encrypt(str: String): String = SM3.encrypt(str)
    //20230725 add end

    protected def tuomin(str: String): String = {
        println("test----------tuomin")
        val n = if (str.length % 2 == 0) str.length / 2
        else str.length / 2 + 1
        var s = ""
        var i = 0
        while ( {
            i < n
        }) {
            s += "*"

            {
                i += 1;
                i - 1
            }
        }
        s + str.substring(n, str.length)
    }


    private def genEncryptSql(colnameTypes: java.util.LinkedHashMap[String, String], func: String, tableName: String): String = {

        val newColumns = colnameTypes.asScala.map(col => {
            val column = col._1
            val types = col._2.split(",")
            if (types.length > 2) {
                val encrytType = types(2).toString.toLowerCase
                s"${func}_$encrytType($column) AS $column"
            } else {
                s"$column AS $column"
            }
        })

        val sql = s"select ${newColumns.mkString(",")}  from $tableName"

        println(s"*************encryptor sql = $sql")
        sql
    }

}
