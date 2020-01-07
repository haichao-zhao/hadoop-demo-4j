package com.zhc.bigdata.hadoop.hdfs;

/**
 * 自定义不区分大小写wc实现类
 */
public class CaseIgnoreWordCountMapper implements MyMapper {
    @Override
    public void map(String line, MyContext context) {

        String[] words = line.toLowerCase().split(",");
        for (String word : words) {
            Object value = context.get(word);
            if (value == null) {
                context.write(word, 1);
            } else {
                int i = Integer.parseInt(value.toString());
                context.write(word, i + 1);
            }
        }
    }
}
