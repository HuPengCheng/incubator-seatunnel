package org.apache.seatunnel.spark.encrypt;

import org.apache.commons.codec.binary.Hex;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by Paul-1 on 2018/8/20.
 */
public class SHA256 {

    /**
     * 利用Apache的工具类实现SHA-256加密 所需jar包下載 http://pan.baidu.com/s/1nuKxYGh
     *
     * @param str 加密后的报文
     */
    public static String SHA256Encrypt(String str) {
        MessageDigest messageDigest;
        String encdeStr = "";
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            byte[] hash = messageDigest.digest(str.getBytes("UTF-8"));
            encdeStr = Hex.encodeHexString(hash);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return encdeStr;
    }

}
