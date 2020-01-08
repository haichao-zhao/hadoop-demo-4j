package com.zhc.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * MapReduce 自定义分区规则
 * 需求：将统计结果按照手机号的前缀进行区分，并输出到不同的输出文件中去
 * 13* ==> ..
 * 15* ==> ..
 * other ==> ..
 */
public class AccessPartitioner extends Partitioner<Text, Access> {
    @Override
    public int getPartition(Text phone, Access access, int numPartitions) {
        if (phone.toString().startsWith("13")) {
            return 0;
        } else if (phone.toString().startsWith("15")) {
            return 1;
        } else {
            return 2;
        }
    }
}
