package org.apache.seatunnel.spark.hudi.etl.type;

import com.google.common.base.Preconditions;

/**
 * bug fixed
 *
 * @author yangyang.fu
 * @date Oct 28, 2015 11:58:10 PM
 */
class StringParser extends AbstractTypeParser<String> {

    public String parse() {
        String strValue = "";
        if ((this.value instanceof String)) {
            strValue = (String) this.value;
        } else {
            strValue = String.valueOf(this.value);
        }

        return getSubIfMust(strValue);

    }

    /**
     * bug fixed. 汉字字符与DB2中varchar不一个概念，导致越长。
     */
    private String getSubIfMust(String strValue) {
        int byteLenLimit = TYPE_CONST.DEFAULT_STRING_LENGTH;
        if (null != this.specs) {
            byteLenLimit = Integer.parseInt(this.specs);
        }

        Preconditions.checkArgument(byteLenLimit >= 0,
            this.specs + " must not be negative! value=" + this.value);

        int strLen = strValue.length();
        if (isAllAscii(strValue)) {// 全是码
            if (strLen > byteLenLimit) {
                return strValue.substring(0, byteLenLimit);
            }
            return strValue;
        } else {// 有汉字等其它
            for (int i = strLen; i > 0; i--) {
                String substr = strValue.substring(0, i);
                if (substr.getBytes().length <= byteLenLimit) {
                    return substr;
                }
            }
        }
        throw new RuntimeException(
            "ruleDesc:" + this.ruleDesc + ";specs:" + this.specs + ";strValue:" + strValue
                + "  Error in StringParser!");
    }

    /**
     * 全是单字节字符
     */
    private boolean isAllAscii(String s) {
        return s.getBytes().length == s.length();
    }
}