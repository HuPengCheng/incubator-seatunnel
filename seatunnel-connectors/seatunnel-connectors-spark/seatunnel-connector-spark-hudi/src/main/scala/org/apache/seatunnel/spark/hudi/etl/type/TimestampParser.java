package org.apache.seatunnel.spark.hudi.etl.type;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;


class TimestampParser extends AbstractTypeParser<Timestamp> {

    public Timestamp parse() {
        String format = TYPE_CONST.DEFAULT_TIMESTAMP_FORMAT;
        if (null != this.specs) {
            format = this.specs;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
            if ((this.value instanceof Timestamp)) {
                return new Timestamp(sdf.parse(sdf.format(this.value)).getTime());
            }
            String strValue = String.valueOf(this.value).trim();
            int len = format.length();
            if (strValue.length() > len) {
                strValue = strValue.substring(0, len);
            }

            return new Timestamp(sdf.parse(strValue).getTime());
        } catch (ParseException e) {
//      Log.error(new Object[] { "Error when parsing a timestamp", e });
        }
        return null;
    }

    public static void main(String[] args) {
        AbstractTypeParser<Timestamp> parser = new TimestampParser();
        //Timestamp d = new Timestamp(System.currentTimeMillis());
        String d = "2016-02-26 02:02:00.0";
        parser.setValue(d);
        parser.setSpecs("timestamp,yyyy-MM-dd");
        System.out.println(parser.parse());
    }
}