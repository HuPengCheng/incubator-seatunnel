package org.apache.seatunnel.spark.encrypt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
 * @author: yp-tc-m-4814
 * @date: 2020-02-04 14:33
 */
object SqlUdf {

    val aesKey = "I am a fool, OK?"
    val sjbKey = "3b8232ddf5354545a94a5d6b3263f7c3"
    def register(sparkSession: SparkSession):UserDefinedFunction = {
        registerBitANdUdf(sparkSession)
        registerEncrypt(sparkSession)
        registerDecrypt(sparkSession)
        registerSJBDecrypt(sparkSession)
        registerSJBEncrypt(sparkSession)
    }

    private def registerBitANdUdf(sparkSession: SparkSession):UserDefinedFunction = {
        val bitand = (left: AnyVal, right: Integer) => {
            val intLeft = Integer.parseInt(left.toString)
            intLeft & right
        }
        sparkSession.udf.register("bit_and_tmp", bitand)
    }

    private def registerEncrypt(sparkSession: SparkSession):UserDefinedFunction = {
        val encrypt = (value : String) => {
            try {
                AES.encryptToBase64(value,aesKey)
            }
            catch {
                case ex:Exception => "加密失败"
            }
        }
        sparkSession.udf.register("dataEncrypt",encrypt)
    }

    private def registerDecrypt(sparkSession: SparkSession):UserDefinedFunction = {
        val decrypt = (value : String) => {
            try {
                AES.decryptFromBase64(value,aesKey)
            }
            catch {
                case ex:Exception => "解密失败"
            }
        }
        sparkSession.udf.register("dataDecrypt",decrypt)
    }

    private def registerSJBEncrypt(sparkSession: SparkSession):UserDefinedFunction = {
        val sjbEncrypt = (value : String) => {
            try {
                AEScrypt.encryptAES(value,sjbKey)
            }
            catch {
                case ex:Exception => "加密失败"
            }
        }
        sparkSession.udf.register("sjbEncrypt",sjbEncrypt)
    }

    private def registerSJBDecrypt(sparkSession: SparkSession):UserDefinedFunction = {
        val sjbDecrypt = (value : String) => {
            try {
                AEScrypt.decryptAES(value,sjbKey)
            }
            catch {
                case ex:Exception => "解密失败"
            }
        }
        sparkSession.udf.register("sjbDecrypt",sjbDecrypt)
    }
}
