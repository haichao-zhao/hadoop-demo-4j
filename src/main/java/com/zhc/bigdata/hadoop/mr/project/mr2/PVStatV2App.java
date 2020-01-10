package com.zhc.bigdata.hadoop.mr.project.mr2;


import com.zhc.bigdata.hadoop.mr.project.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * 第一个版本的流量统计
 */
public class PVStatV2App {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境

        //创建Configuration
        Configuration conf = new Configuration();

        //创建Job
        Job job = Job.getInstance(conf);
        //设置执行Job的主类
        job.setJarByClass(PVStatV2App.class);

        //设置map/reduce执行类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置map输出类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce输出类
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        //设置输入/输出位置
        //如果输出路径已经存在，先删除
        Path outpath = new Path("output/v2/pv");
        FileUtils.FileExistsParser(conf,outpath);

        FileInputFormat.setInputPaths(job, new Path("input/etl/"));
        FileOutputFormat.setOutputPath(job, outpath);

        //执行Job
        job.waitForCompletion(true);
    }

    /**
     * 自定义mapper类
     * 输入KEY    偏移量      LongWritable
     * 输入VALUE  每行数据    Text
     * 输出KEY    处理后的KEY Text
     * 输出VALUE  数量       LongWritable
     */
    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private Text KEY = new Text("key");
        private LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.write(KEY, ONE);
        }
    }


    /**
     * 自定义reducer类
     * 输入KEY    mapper类的输出KEY
     * 输入VALUE  mapper类的输出VALUE
     * 输出KEY    处理后的KEY NullWritable
     * 输出VALUE  数量       LongWritable
     */
    static class MyReducer extends Reducer<Text, LongWritable, NullWritable, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long count = 0;
            for (LongWritable value : values) {
                count++;
            }
            context.write(NullWritable.get(), new LongWritable(count));

        }
    }

}

