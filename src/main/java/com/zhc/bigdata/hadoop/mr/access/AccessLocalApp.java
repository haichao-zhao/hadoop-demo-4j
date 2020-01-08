package com.zhc.bigdata.hadoop.mr.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AccessLocalApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //创建一个作业
        Job job = Job.getInstance(conf);
        //设置执行作业的主类
        job.setJarByClass(AccessLocalApp.class);

        //设置mapper的执行类
        job.setMapperClass(AccessMapper.class);
        //设置reducer的执行类
        job.setReducerClass(AccessReducer.class);

        //设置map的 <K,V> 输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        //设置reduce的 <K,V> 输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        //设置自定义分区规则
        job.setPartitionerClass(AccessPartitioner.class);

        //设置reduce分区个数
        job.setNumReduceTasks(3);

        //如果输出路径已经存在，先删除
        Path outpath = new Path("access/output/");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outpath)) {
            fs.delete(outpath, true);
        }

        //设置作业的输入输出路径
        FileInputFormat.setInputPaths(job, new Path("access/input/"));
        FileOutputFormat.setOutputPath(job, outpath);

        //提交Job
        job.waitForCompletion(true);

    }
}
