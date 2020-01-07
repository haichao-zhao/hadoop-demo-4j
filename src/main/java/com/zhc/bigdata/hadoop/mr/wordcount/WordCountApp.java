package com.zhc.bigdata.hadoop.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class WordCountApp {
    public static void main(String[] args) throws Exception {

        // 设置hadoop用户，如果本地执行这个程序，本机用户名没有操作hadoop权限，会报错
        // System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:8020");

        //创建一个工作
        Job job = Job.getInstance(conf);

        //设置job对应的参数:主类/执行类
        job.setJarByClass(WordCountApp.class);

        //设置自定义的map和reduce处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reducer输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果输出路径已经存在，先删除
        Path outpath = new Path("/test/worldCount");
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:8020"), conf);
        if (fs.exists(outpath)) {
            fs.delete(outpath, true);
        }

        //设置作业的输入输出路径
        FileInputFormat.setInputPaths(job, new Path("/test/HR.csv"));
        FileOutputFormat.setOutputPath(job, outpath);

        //提交Job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);
    }
}
