package org.apache.seatunnel.spark.hudi.etl.type;

class FloatParser extends AbstractTypeParser<Float> {

    public Float parse() {
        if ((this.value instanceof Float)) {
            return (Float) this.value;
        }
        return Float.valueOf(String.valueOf(this.value).trim());
    }
}