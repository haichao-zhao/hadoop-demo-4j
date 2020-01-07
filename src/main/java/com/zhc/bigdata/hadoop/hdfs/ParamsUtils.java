package com.zhc.bigdata.hadoop.hdfs;

import java.io.IOException;
import java.util.Properties;

/**
 * 读取配置文件
 */
public class ParamsUtils {
    private static Properties properties = new Properties();

    static {
        try {
            properties.load(ParamsUtils.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties() {
        return properties;
    }
}
