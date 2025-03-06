package org.apache.seatunnel.spark.hudi.etl.type;

class IntegerParser extends AbstractTypeParser<Integer> {

    public Integer parse() {
        if ((this.value instanceof Integer)) {
            return (Integer) this.value;
        }
        return Integer.valueOf(String.valueOf(this.value).trim());
    }
}