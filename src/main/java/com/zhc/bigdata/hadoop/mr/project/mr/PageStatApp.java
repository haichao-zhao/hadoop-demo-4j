package com.zhc.bigdata.hadoop.mr.project.mr;

import com.zhc.bigdata.hadoop.mr.project.utils.FileUtils;
import com.zhc.bigdata.hadoop.mr.project.utils.GetPageId;
import com.zhc.bigdata.hadoop.mr.project.utils.LogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Map;

public class PageStatApp {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境

        //创建Configuration
        Configuration conf = new Configuration();

        //创建Job
        Job job = Job.getInstance(conf);

        //设置执行Job的主类
        job.setJarByClass(PageStatApp.class);

        //设置mapper/reducer 执行类
        job.setMapperClass(PageMapper.class);
        job.setReducerClass(PageReducer.class);

        //设置mapper输出<K,V>类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reducer输出<K,V>类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置输入输出路径
        Path outpath = new Path("output/v1/page_pv");
        FileUtils.FileExistsParser(conf, outpath);

        FileInputFormat.setInputPaths(job, new Path("input/"));
        FileOutputFormat.setOutputPath(job, outpath);

        //提交Job
        job.waitForCompletion(true);

    }

    //自定义mapper类
    static class PageMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static LongWritable ONE = new LongWritable(1);

        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String s = value.toString();
            Map<String, String> parse = logParser.parse(s);
            String url = parse.get("url");
            String pageId = GetPageId.getPageId(url);

            context.write(new Text(pageId), ONE);
        }
    }

    //自定义reducer类
    static class PageReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long count = 0;
            for (LongWritable value : values) {
                count++;
            }
            context.write(key, new LongWritable(count));
        }
    }


}
