package org.apache.seatunnel.spark.hudi.etl.type;

import com.google.common.base.Strings;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

/**
 * 类型转换器
 *
 * @author yangyang.fu
 * @date Nov 18, 2015 2:24:27 PM
 */
public class TypeParser implements Serializable {

    private static final long serialVersionUID = 6097727367006130915L;

    private final StringParser STRING_PARSER = new StringParser();
    private final BigDecimalParser BIGDECIMAL_PARSER = new BigDecimalParser();
    private final BooleanParser BOOLEAN_PARSER = new BooleanParser();
    private final DateParser DATE_PARSER = new DateParser();
    private final DoubleParser DOUBLE_PARSER = new DoubleParser();
    private final FloatParser FLOAT_PARSER = new FloatParser();
    private final IntegerParser INTEGER_PARSER = new IntegerParser();
    private final LongParser LONG_PARSER = new LongParser();
    private final TimestampParser TIMESTAMP_PARSER = new TimestampParser();
    private final DefaultParser Default_PARSER = new DefaultParser();

    public Object parse(String colDesc, Object value) {
        if (null == value) {
            return null;
        }
        if (Strings.isNullOrEmpty(colDesc)) {
            colDesc = "string";
        } else {
            colDesc = colDesc.toLowerCase();
        }

        AbstractTypeParser<?> parser = null;
        if (colDesc.startsWith("string")) {
            STRING_PARSER.setSpecs(colDesc);
            STRING_PARSER.setValue(value);
            parser = STRING_PARSER;
        } else if ("long".equalsIgnoreCase(colDesc)) {
            LONG_PARSER.setSpecs(colDesc);
            LONG_PARSER.setValue(value);
            parser = LONG_PARSER;
        } else if (colDesc.startsWith("timestamp")) {
            TIMESTAMP_PARSER.setSpecs(colDesc);
            TIMESTAMP_PARSER.setValue(value);
            parser = TIMESTAMP_PARSER;
        } else if (colDesc.startsWith("date")) {
            DATE_PARSER.setSpecs(colDesc);
            DATE_PARSER.setValue(value);
            parser = DATE_PARSER;
        } else if ("integer".equalsIgnoreCase(colDesc)) {
            INTEGER_PARSER.setSpecs(colDesc);
            INTEGER_PARSER.setValue(value);
            parser = INTEGER_PARSER;
        } else if ("boolean".equalsIgnoreCase(colDesc)) {
            BOOLEAN_PARSER.setSpecs(colDesc);
            BOOLEAN_PARSER.setValue(value);
            parser = BOOLEAN_PARSER;
        } else if ("bigdecimal".equalsIgnoreCase(colDesc)) {
            BIGDECIMAL_PARSER.setSpecs(colDesc);
            BIGDECIMAL_PARSER.setValue(value);
            parser = BIGDECIMAL_PARSER;
        } else if ("double".equalsIgnoreCase(colDesc)) {
            DOUBLE_PARSER.setSpecs(colDesc);
            DOUBLE_PARSER.setValue(value);
            parser = DOUBLE_PARSER;
        } else if ("float".equalsIgnoreCase(colDesc)) {
            FLOAT_PARSER.setSpecs(colDesc);
            FLOAT_PARSER.setValue(value);
            parser = FLOAT_PARSER;
        } else {
            Default_PARSER.setSpecs(colDesc);
            Default_PARSER.setValue(value);
            parser = Default_PARSER;
        }

        return parser.parse();
    }


    public String parseString(String colDesc, Object value) {
        if (null == value) {
            return null;
        }
        STRING_PARSER.setSpecs(colDesc);
        STRING_PARSER.setValue(value);
        return STRING_PARSER.parse();
    }


    public Long parseLong(String colDesc, Object value) {
        if (null == value) {
            return null;
        }
        LONG_PARSER.setSpecs(colDesc);
        LONG_PARSER.setValue(value);
        return LONG_PARSER.parse();
    }

    public Timestamp parseTimestamp(String colDesc, Object value) {
        if (null == value) {
            return null;
        }
        TIMESTAMP_PARSER.setSpecs(colDesc);
        TIMESTAMP_PARSER.setValue(value);
        return TIMESTAMP_PARSER.parse();
    }

    public Date parseDate(String colDesc, Object value) {
        if (null == value) {
            return null;
        }
        DATE_PARSER.setSpecs(colDesc);
        DATE_PARSER.setValue(value);
        return DATE_PARSER.parse();
    }


    public Integer parseInteger(String colDesc, Object value) {
        if (null == value) {
            return null;
        }
        INTEGER_PARSER.setSpecs(colDesc);
        INTEGER_PARSER.setValue(value);
        return INTEGER_PARSER.parse();
    }


    public Boolean parseBoolean(String colDesc, Object value) {
        if (null == value) {
            return null;
        }
        BOOLEAN_PARSER.setSpecs(colDesc);
        BOOLEAN_PARSER.setValue(value);
        return BOOLEAN_PARSER.parse();
    }


    public BigDecimal parseBigdecimal(String colDesc, Object value) {
        if (null == value) {
            return null;
        }
        BIGDECIMAL_PARSER.setSpecs(colDesc);
        BIGDECIMAL_PARSER.setValue(value);
        return BIGDECIMAL_PARSER.parse();
    }

    public Double parseDouble(String colDesc, Object value) {
        if (null == value) {
            return null;
        }
        DOUBLE_PARSER.setSpecs(colDesc);
        DOUBLE_PARSER.setValue(value);
        return DOUBLE_PARSER.parse();
    }

    public Float parseFloat(String colDesc, Object value) {
        if (null == value) {
            return null;
        }
        FLOAT_PARSER.setSpecs(colDesc);
        FLOAT_PARSER.setValue(value);
        return FLOAT_PARSER.parse();
    }
}