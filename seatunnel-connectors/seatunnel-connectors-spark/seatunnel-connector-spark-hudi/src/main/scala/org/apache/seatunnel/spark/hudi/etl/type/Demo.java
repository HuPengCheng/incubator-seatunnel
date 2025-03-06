package org.apache.seatunnel.spark.hudi.etl.type;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author: wenxiang.wu
 * @date: 2018/10/25 下午4:53
 */
public class Demo {

    public static void main(String[] args) throws ParseException {

        String format = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date date = sdf.parse("2012-07-10 14:58:00.000000");
        System.out.println(date.toString());

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println(timestamp.toString());
    }


}