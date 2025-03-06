package org.apache.seatunnel.spark.hudi.etl.type;


public class TypeConst {

    public final int DEFAULT_STRING_LENGTH;
    public final String DEFAULT_DATE_FORMAT;
    public final String DEFAULT_TIMESTAMP_FORMAT;

    public TypeConst(PropertyLoader loader) {
        this.DEFAULT_STRING_LENGTH = new Integer(
            loader.getValue("DEFAULT_STRING_LENGTH", String.valueOf(Integer.MAX_VALUE)))
            .intValue();
        this.DEFAULT_DATE_FORMAT = loader.getValue("DEFAULT_DATE_FORMAT", "yyyy-MM-dd");
        this.DEFAULT_TIMESTAMP_FORMAT = loader
            .getValue("DEFAULT_TIMESTAMP_FORMAT", "yyyy-MM-dd HH:mm:ss");
    }

}