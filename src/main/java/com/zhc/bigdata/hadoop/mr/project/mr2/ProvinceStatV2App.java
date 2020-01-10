package com.zhc.bigdata.hadoop.mr.project.mr2;

import com.zhc.bigdata.hadoop.mr.project.utils.FileUtils;
import com.zhc.bigdata.hadoop.mr.project.utils.IPParser;
import com.zhc.bigdata.hadoop.mr.project.utils.LogParser;
import org.apache.commons.lang.StringUtils;
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

/**
 * 统计各省份页面访问量
 */
public class ProvinceStatV2App {
    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境

        //创建Configuration
        Configuration conf = new Configuration();

        //创建Job
        Job job = Job.getInstance(conf);
        //设置执行Job的主类
        job.setJarByClass(ProvinceStatV2App.class);

        //设置map/reduce执行类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置map输出类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce输出类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置输入/输出位置
        //如果输出路径已经存在，先删除
        Path outpath = new Path("output/v2/province_pv");
        FileUtils.FileExistsParser(conf, outpath);

        FileInputFormat.setInputPaths(job, new Path("input/etl/"));
        FileOutputFormat.setOutputPath(job, outpath);

        //执行Job
        job.waitForCompletion(true);

    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private LongWritable ONE = new LongWritable(1);

        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Map<String, String> info = logParser.parse2(value.toString());

            String province = info.get("province");

            context.write(new Text(province), ONE);

        }
    }

    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
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
