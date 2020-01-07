package com.zhc.bigdata.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义上下文对象
 */
public class MyContext {
    private Map<Object, Object> cacheMap = new HashMap<>();

    public Map<Object, Object> getCacheMap() {
        return cacheMap;
    }

    /**
     * 写数据到缓存中
     *
     * @param key   单词
     * @param value 词频
     */
    public void write(Object key, Object value) {
        cacheMap.put(key, value);
    }

    /**
     * 获取单词词频
     *
     * @param key 单词
     * @return 词频
     */
    public Object get(Object key) {
        return cacheMap.get(key);
    }
}
