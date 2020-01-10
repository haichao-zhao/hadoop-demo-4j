package com.zhc.bigdata.hadoop.mr.project.mr2;

import com.zhc.bigdata.hadoop.mr.project.utils.FileUtils;
import com.zhc.bigdata.hadoop.mr.project.utils.GetPageId;
import com.zhc.bigdata.hadoop.mr.project.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Map;

public class ETLApp {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境

        //创建Configuration
        Configuration conf = new Configuration();

        //创建Job
        Job job = Job.getInstance(conf);

        //设置执行Job的主类
        job.setJarByClass(ETLApp.class);

        //设置mapper/reducer 执行类
        job.setMapperClass(ETLMapper.class);


        //设置mapper输出<K,V>类
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);


        //设置输入输出路径
        Path outpath = new Path(args[1]);
        outpath.getFileSystem(conf).delete(outpath, true);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outpath);

        //提交Job
        job.waitForCompletion(true);

    }

    //自定义mapper类
    static class ETLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String log = value.toString();
            Map<String, String> logInfo = logParser.parse(log);

            String ip = logInfo.get("ip");
            String url = logInfo.get("url");
            String sessionId = logInfo.get("sessionId");
            String time = logInfo.get("time");
            String country = logInfo.get("country") == null ? "-" : logInfo.get("country");
            String province = logInfo.get("province") == null ? "-" : logInfo.get("province");
            String city = logInfo.get("city") == null ? "-" : logInfo.get("city");
            String pageId = GetPageId.getPageId(url) == "" ? "-" : GetPageId.getPageId(url);

            StringBuilder builder = new StringBuilder();
            builder.append(ip).append("\t");
            builder.append(url).append("\t");
            builder.append(sessionId).append("\t");
            builder.append(time).append("\t");
            builder.append(country).append("\t");
            builder.append(province).append("\t");
            builder.append(city).append("\t");
            builder.append(pageId);


            context.write(NullWritable.get(), new Text(builder.toString()));
        }
    }

}
