package org.apache.seatunnel.spark.hudi.etl.type;

import com.google.common.base.Preconditions;

import java.io.Serializable;

abstract class AbstractTypeParser<T> implements Serializable {

    public static final TypeConst TYPE_CONST = new TypeConst(new PropertyLoader(
        "prop/typeconst.txt"));
    private static final long serialVersionUID = -69664653263424474L;

    protected String specs = null;
    protected Object value;
    protected String ruleDesc;

    public abstract T parse();

    public void setSpecs(String colDesc) {
        ruleDesc = colDesc;
        String[] splits = colDesc.split(",");

        Preconditions.checkArgument(splits.length <= 2,
            "Error colNameType configured in econf.xml");

        if (splits.length == 2) {
            this.specs = splits[1];
        } else {
            this.specs = null;
        }
    }

    public void setValue(Object value) {
        this.value = value;
    }
}