package com.zhc.bigdata.hadoop.hdfs;

/**
 * 自定义mapper
 */
public interface MyMapper {
    /**
     * @param line    每一行数据
     * @param context 上下文/缓存
     */
    public void map(String line, MyContext context);
}
