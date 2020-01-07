package com.zhc.bigdata.hadoop.hdfs;

/**
 * 自定义wc实现类
 */
public class WordCountMapper implements MyMapper {
    @Override
    public void map(String line, MyContext context) {

        String[] words = line.split(",");
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
