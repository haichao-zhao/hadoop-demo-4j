package com.zhc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * 使用Java API 操作HDFS
 */
public class HDFSApp {
    public static void main(String[] args) throws Exception {


//        Configuration conf = new Configuration();
//        URI uri = new URI("hdfs://localhost:8020");
//        FileSystem fileSystem = FileSystem.get(uri, conf);
//        Path f = new Path("/hdfsapi/java/test");
//        boolean mkdirs = fileSystem.mkdirs(f);

//        System.out.println(mkdirs);
    }

    private static final String HDFS_PATH = "hdfs://localhost:8020";
    FileSystem fileSystem = null;
    Configuration conf = null;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        conf.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI(HDFS_PATH), conf);
        System.out.println("________setUp_______");
    }

    /**
     * 创建HDFS文件夹
     *
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception {
        boolean mkdirs = fileSystem.mkdirs(new Path("/hdfsapi/java/test"));

        System.out.println(mkdirs);
    }

    /**
     * 读取HDFS文件内容
     *
     * @throws Exception
     */
    @Test
    public void text() throws Exception {
        FSDataInputStream open = fileSystem.open(new Path("/test/HR.csv"));
        IOUtils.copyBytes(open, System.out, 1024);
    }

    /**
     * 创建文件
     *
     * @throws Exception
     */
    @Test
    public void create() throws Exception {

        FSDataInputStream in = fileSystem.open(new Path("/test/HR.csv"));
        FSDataOutputStream out = fileSystem.create(new Path("/test/HR3.csv"));
        IOUtils.copyBytes(in, out, 1024);
    }

    /**
     * 文件重命名
     *
     * @throws Exception
     */
    @Test
    public void rename() throws Exception {
        Path oldname = new Path("/test/HR3.csv");
        Path newname = new Path("/test/HR2.csv");
        boolean rename = fileSystem.rename(oldname, newname);

        System.out.println(rename);
    }

    /**
     * 复制本地文件到HDFS
     *
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path("pom.xml");
        Path hdfsPath = new Path("/test/pom.xml");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 复制HDFS文件到本地
     *
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path localPath = new Path("src");
        Path hdfsPath = new Path("/test/HR2.csv");
        fileSystem.copyToLocalFile(hdfsPath, localPath);
    }

    /**
     * 查看目标文件夹下文件信息
     *
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] statuses = fileSystem.listStatus(new Path("/test/"));
        for (FileStatus s : statuses) {
            String isDir = s.isDirectory() ? "文件夹" : "文件";
            String permission = s.getPermission().toString();
            short replication = s.getReplication();
            long len = s.getLen();
            String path = s.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + len + "\t" + path);

        }
    }


    /**
     * 查看文件块信息
     *
     * @throws Exception
     */
    @Test
    public void getBlockLocations() throws Exception {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/test/2019.11.27.19.evt"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        for (BlockLocation block : blocks) {
            for (String name : block.getNames()) {
                System.out.println(name + " : " + block.getOffset() + " : " + block.getLength());
            }
        }
    }

    /**
     * 删除文件
     *
     * @throws Exception
     */
    @Test
    public void delete() throws Exception {

        boolean delete = fileSystem.delete(new Path("/test/HR2.csv"), false);
        System.out.println(delete);
    }

    @After
    public void tearDown() throws Exception {
        fileSystem.close();
        conf = null;
        fileSystem = null;
        System.out.println("________tearDown_______");
    }

}
