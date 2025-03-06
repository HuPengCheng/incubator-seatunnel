package org.apache.seatunnel.spark.hudi.etl.type;

class LongParser extends AbstractTypeParser<Long> {

    public Long parse() {
        if (!(this.value instanceof Long)) {
            return Long.valueOf(String.valueOf(this.value).trim());
        }
        return (Long) this.value;
    }
}