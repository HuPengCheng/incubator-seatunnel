package org.apache.seatunnel.spark.encrypt;

import org.apache.commons.codec.binary.Base64;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MD5加密工具
 */
public class MD5 {

    // MD5
    private static final char[] DIGITS = {'0', '1', '2', '3', '4', '5', '6',
        '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private static String base64(String s) {
        return new String(Base64.encodeBase64(s.getBytes()));
    }

    public static String md5x(String text) {
        return md5(base64(text));
    }

    public static String md5(String text) {
        return md5(new String[]{text});
    }

    private static String md5(String[] text) {
        byte[] bytes = digest(text);
        return new String(encodeHex(bytes));
    }

    private static byte[] digest(String... text) {
        MessageDigest msgDigest = null;
        try {
            msgDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                "System doesn't support MD5 algorithm.");
        }

        try {
            for (String str : text) {
                msgDigest.update(str.getBytes("utf-8"));
            }

        } catch (UnsupportedEncodingException e) {

            throw new IllegalStateException(
                "System doesn't support your  EncodingException.");

        }

        return msgDigest.digest();
    }

    private static char[] encodeHex(byte[] data) {

        int l = data.length;

        char[] out = new char[l << 1];

        // two characters form the hex value.
        for (int i = 0, j = 0; i < l; i++) {
            out[j++] = DIGITS[(0xF0 & data[i]) >>> 4];
            out[j++] = DIGITS[0x0F & data[i]];
        }
        return out;
    }

}