package org.apache.seatunnel.spark.encrypt;
import org.bouncycastle.crypto.digests.SM3Digest;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.pqc.math.linearalgebra.ByteUtils;

/**
 * SM3算法（类似MD5），不可逆
 *
 * @author chunrong.wang
 * @date 2023年07月24日 下午8:09:01
 */
public class SM3 {


    /**
     * SM3加密方式之：不提供密钥的方式 SM3加密，返回加密后长度为64位的16进制字符串
     * 这个和白玮的加密方式一致
     * @param src 明文
     * @return
     */
    public static String encrypt(String src) {
        return ByteUtils.toHexString(getEncryptBySrcByte(src.getBytes())).toUpperCase();
    }

    /**
     * 返回长度为32位的加密后的byte数组
     *
     * @param srcByte
     * @return
     */
    public static byte[] getEncryptBySrcByte(byte[] srcByte) {
        SM3Digest sm3 = new SM3Digest();
        sm3.update(srcByte, 0, srcByte.length);
        byte[] encryptByte = new byte[sm3.getDigestSize()];
        sm3.doFinal(encryptByte, 0);
        return encryptByte;
    }

    /**
     * sm3算法加密
     *
     * @param paramStr 待加密字符串
     * @return 返回加密后，固定长度=32的16进制字符串
     */
    public static String encryptHexString(String paramStr) {
        byte[] resultHash = hash(paramStr.getBytes());
        return ByteUtils.toHexString(resultHash);
    }

    /**
     * 返回长度=32的byte数组
     *
     * @param srcData
     * @return
     * @explain 生成对应的hash值
     */
    private static byte[] hash(byte[] srcData) {
        SM3Digest digest = new SM3Digest();
        digest.update(srcData, 0, srcData.length);
        byte[] hash = new byte[digest.getDigestSize()];
        digest.doFinal(hash, 0);
        return hash;
    }

    /**
     * 通过密钥进行加密
     *
     * @param keyBytes 密钥
     * @param srcData  被加密的byte数组
     * @return
     * @explain 指定密钥进行加密
     */
    public static byte[] hashByKey(byte[] keyBytes, byte[] srcData) {
        KeyParameter keyParameter = new KeyParameter(keyBytes);
        SM3Digest digest = new SM3Digest();
        HMac mac = new HMac(digest);
        mac.init(keyParameter);
        mac.update(srcData, 0, srcData.length);
        byte[] result = new byte[mac.getMacSize()];
        mac.doFinal(result, 0);
        return result;
    }
}


