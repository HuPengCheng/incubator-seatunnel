package org.apache.seatunnel.spark.hudi.etl.type;

class DoubleParser extends AbstractTypeParser<Double> {

    public Double parse() {
        if ((this.value instanceof Double)) {
            return (Double) this.value;
        }
        return Double.valueOf(String.valueOf(this.value).trim());
    }
}