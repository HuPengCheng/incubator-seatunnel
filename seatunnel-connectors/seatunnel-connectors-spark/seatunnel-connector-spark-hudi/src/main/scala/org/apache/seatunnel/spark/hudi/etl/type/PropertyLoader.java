package org.apache.seatunnel.spark.hudi.etl.type;

import org.apache.log4j.Logger;

import java.util.Properties;

public class PropertyLoader {

    private static final Logger LOG = Logger.getLogger(PropertyLoader.class);
    private Properties p = new Properties();

    public PropertyLoader(String propFilePath) {
        load(propFilePath);
    }

    public void load(String propFilePath) {
//        if (null == propFilePath) {
//            throw new NullPointerException("Path of the property file is null");
//        }
//        InputStream in = null;
//        try {
//            in = new BufferedInputStream(new FileInputStream(propFilePath));
//            this.p.load(in);
//
//            if (null != in)
//                try {
//                    in.close();
//                } catch (IOException e3) {
//                    LOG.error("", e3);
//                }
//        } catch (IOException e) {
//            LOG.error("", e);
//
//            if (null != in)
//                try {
//                    in.close();
//                } catch (IOException e3) {
//                    LOG.error("", e3);
//                }
//        } finally {
//            if (null != in)
//                try {
//                    in.close();
//                } catch (IOException e3) {
//                    LOG.error("", e3);
//                }
//        }
    }

    public String getValue(String key, String dftValue) {
        String p_value = this.p.getProperty(key);
        return null == p_value ? dftValue : p_value;
    }
}