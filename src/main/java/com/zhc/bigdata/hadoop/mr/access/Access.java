package com.zhc.bigdata.hadoop.mr.access;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义复杂数据类型
 * 1）按照Hadoop的规范，需要实现Writable接口
 * 2）按照hadoop的规范，需要实现write和readFields方法
 * 3）生成一个无参的构造方法
 */
public class Access implements Writable {
    private String phoneNum;

    private long upAccess;

    private long downAccess;

    private long totalAccess;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNum);
        out.writeLong(upAccess);
        out.writeLong(downAccess);
        out.writeLong(totalAccess);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phoneNum = in.readUTF();
        this.upAccess = in.readLong();
        this.downAccess = in.readLong();
        this.totalAccess = in.readLong();
    }

    public Access() {
    }

    public Access(String phoneNum, long upAccess, long downAccess) {
        this.phoneNum = phoneNum;
        this.upAccess = upAccess;
        this.downAccess = downAccess;
        this.totalAccess = upAccess + downAccess;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public long getUpAccess() {
        return upAccess;
    }

    public void setUpAccess(long upAccess) {
        this.upAccess = upAccess;
    }

    public long getDownAccess() {
        return downAccess;
    }

    public void setDownAccess(long downAccess) {
        this.downAccess = downAccess;
    }

    public long getTotalAccess() {
        return totalAccess;
    }

    public void setTotalAccess(long totalAccess) {
        this.totalAccess = totalAccess;
    }

    @Override
    public String toString() {
        return phoneNum +
                "," + upAccess +
                "," + downAccess +
                "," + totalAccess;
    }


}
