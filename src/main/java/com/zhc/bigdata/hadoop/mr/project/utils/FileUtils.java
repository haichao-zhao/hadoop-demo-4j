package com.zhc.bigdata.hadoop.mr.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUtils {
    public static void FileExistsParser(Configuration conf, Path outpath) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outpath)) {
            fs.delete(outpath, true);
        }
    }
}
