package org.apache.seatunnel.spark.hudi.etl.type;

public class DefaultParser extends AbstractTypeParser<Object> {

    @Override
    public Object parse() {
        return this.value;
    }

}
