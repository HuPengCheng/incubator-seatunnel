package org.apache.seatunnel.spark.hudi.etl.type;

class BooleanParser extends AbstractTypeParser<Boolean> {

    public Boolean parse() {
        if ((this.value instanceof Boolean)) {
            return (Boolean) this.value;
        }
        return Boolean.valueOf(String.valueOf(this.value).trim());
    }
}