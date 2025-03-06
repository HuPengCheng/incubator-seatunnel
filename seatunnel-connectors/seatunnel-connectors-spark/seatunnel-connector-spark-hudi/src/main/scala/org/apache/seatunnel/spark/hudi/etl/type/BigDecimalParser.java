package org.apache.seatunnel.spark.hudi.etl.type;

import java.math.BigDecimal;

class BigDecimalParser extends AbstractTypeParser<BigDecimal> {

    public BigDecimal parse() {
        if ((this.value instanceof BigDecimal)) {
            return (BigDecimal) this.value;
        }
        return new BigDecimal(String.valueOf(this.value).trim());
    }


    public static void main(String[] args) {
        BigDecimalParser parser = new BigDecimalParser();
        parser.setSpecs("BigDecimal,3");
        parser.setValue("23.1234567");
        parser.parse();
    }
}