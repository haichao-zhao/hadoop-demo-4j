package com.zhc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Set;

/**
 * 使用HDFS API 完成wordcount统计
 * <p>
 * 需求：统计HDFS上的文件的wordcount，然后将统计结果输出到HDFS
 * <p>
 * 功能拆解：
 * 1）读取HDFS上的文件  ==> HDFS API
 * 2）业务处理(词频统计)：对文件中的每一行数据都要进行业务处理(按照分隔符分割)
 * 3）将处理结果缓存起来
 * 4）将结果输出到HDFS     ==> HDFS API
 */
public class HDFSWCApp01 {
    public static void main(String[] args) throws Exception {
        // 1）读取HDFS上的文件  ==> HDFS API
        Path input = new Path("/test/HR.csv");

        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:8020"), conf);
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);

        MyMapper mapper = new WordCountMapper();

        MyContext myContext = new MyContext();

        while (iterator.hasNext()) {
            Path path = iterator.next().getPath();

            FSDataInputStream in = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while ((line = reader.readLine()) != null) {
                // 2）业务处理(词频统计)：对文件中的每一行数据都要进行业务处理(按照分隔符分割)
                mapper.map(line, myContext);
            }

            reader.close();
            in.close();
        }

        // 3）将处理结果缓存起来
        Map<Object, Object> contextMap = myContext.getCacheMap();

        // 4）将结果输出到HDFS     ==> HDFS API
        Path output = new Path("/test/wc.txt");

        FSDataOutputStream out = fs.create(output);

        // 将第三步缓存中的内容输出到out中去
        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
        for (Map.Entry<Object, Object> entry : entries) {
            out.write((entry.getKey().toString() + "\t" + entry.getValue() + "\n").getBytes());
        }

        out.close();
        fs.close();
    }
}
