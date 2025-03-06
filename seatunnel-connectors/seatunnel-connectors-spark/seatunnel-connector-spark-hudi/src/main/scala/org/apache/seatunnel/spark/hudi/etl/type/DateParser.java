package org.apache.seatunnel.spark.hudi.etl.type;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

class DateParser extends AbstractTypeParser<Date> {

    public Date parse() {
        String format = TYPE_CONST.DEFAULT_DATE_FORMAT;
        if (null != this.specs) {
            format = this.specs;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
            if ((this.value instanceof Date)) {
                return sdf.parse(sdf.format(this.value));
            }
            String strValue = String.valueOf(this.value).trim();
            int len = format.length();
            if (strValue.length() > len) {
                strValue = strValue.substring(0, len);
            }
            return sdf.parse(strValue);
        } catch (ParseException e) {
//      Log.error(new Object[] { "Error when parsing a date", e });
        }
        return null;
    }

    public static void main(String[] args) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("date");
        sdf.parse("2020-08-15");
        System.out.println(sdf.parse("2020-08-15"));
    }
}