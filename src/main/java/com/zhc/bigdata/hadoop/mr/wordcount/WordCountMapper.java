package com.zhc.bigdata.hadoop.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //把value对应的行数据按照指定的分隔符分割
        String[] words = value.toString().split(",");

        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }

    }
}
