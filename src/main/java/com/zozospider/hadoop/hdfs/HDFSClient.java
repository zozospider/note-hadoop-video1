package com.zozospider.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * HDFS 客户端
 */
public class HDFSClient {

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {

        System.out.println("begin");

        // 测试在 HDFS 上新建路径
        // test();

        // 上传本地文件到 HDFS
        // copyFromLocalFile();

        // 从 HDFS 拷贝 / 剪切文件到本地
        // copyFromLocalFileWithArgs1();
        // copyFromLocalFileWithArgs2();

        // 从 HDFS 拷贝文件到本地
        // copyToLocalFile();

        // 删除 HDFS 的路径 (文件 / 文件夹)
        // delete();

        // 修改 HDFS 的路径 (文件 / 文件夹) 名称
        // rename();

        // 查看文件详情
        // listFiles();

        // 查看文件状态
        // listStatus();

        // IO 流操作: 上传本地文件到 HDFS
        // ioPutFileToHDFS();

        // IO 流操作: 从 HDFS 拷贝文件到本地
        // ioGetFileFromHDFS();

        // IO 流操作: 从 HDFS 定位读取文件多个部分拷贝到本地
        // ioSeekFileFromHDFS();

        System.out.println("end");
    }

    /**
     * 测试在 HDFS 上新建路径
     * - 需要开放当前客户端连接 193.112.38.200:9000 的权限
     */
    private static void test() throws URISyntaxException, IOException, InterruptedException {

        // 1. 获取 HDFS 客户端对象
        Configuration conf = new Configuration();
        // URI 参数为 NameNode 地址
        FileSystem fs = FileSystem.get(new URI("hdfs://193.112.38.200:9000"), conf, "zozo");

        // 2. 在 HDFS 上创建路径
        fs.mkdirs(new Path("/d2/d2_a"));

        // 3. 关闭资源
        fs.close();

        /**
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_a
         * [zozo@vm017 hadoop-2.7.2]$
         */
    }

    /**
     * 上传本地文件到 HDFS
     * - 需要开放当前客户端连接 193.112.38.200:50010 的权限 (DataNode 服务端口，用于数据传输), 否则会报 RemoteException & IOException 异常
     * - 需要添加代码: conf.set("dfs.client.use.datanode.hostname", "true"); 否则会报 RemoteException & IOException 异常
     * - 需要设置当前客户端机器的 hosts 映射, 否则会报 IOException & UnresolvedAddressException 异常
     */
    private static void copyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {

        /**
         * spiderxmac:tmp zoz$ ls /Users/zoz/zz/other/tmp/f1
         * /Users/zoz/zz/other/tmp/f1
         * spiderxmac:tmp zoz$ cat /Users/zoz/zz/other/tmp/f1
         * I am f1
         * spiderxmac:tmp zoz$
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_a/f1
         * ls: `/d2/d2_a/f1': No such file or directory
         * [zozo@vm017 hadoop-2.7.2]$
         */

        // 1. 获取 HDFS 客户端对象
        Configuration conf = new Configuration();
        // 服务端集群上 DataNode 注册到 NameNode 的地址是本机的 hostname.
        // 当前客户端连接到 NameNode 得到 DataNode 的地址就是对应的 hostname, 因此需要设置当前客户端机器的 hosts 映射.
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(new URI("hdfs://193.112.38.200:9000"), conf, "zozo");

        // 2. 执行上传
        fs.copyFromLocalFile(new Path("/Users/zoz/zz/other/tmp/f1"), new Path("/d2/d2_a/f1"));

        // 3. 关闭资源
        fs.close();

        /**
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_a/f1
         * -rw-r--r--   3 zozo supergroup          8 2019-09-28 17:29 /d2/d2_a/f1
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -cat /d2/d2_a/f1
         * I am f1
         * [zozo@vm017 hadoop-2.7.2]$
         */
    }

    /**
     * 上传本地文件到 HDFS (参数优先级: 客户端配置文件)
     * 优先级: 客户端代码 > 客户端配置文件 > 服务端配置文件 > 服务端默认配置
     */
    private static void copyFromLocalFileWithArgs1() throws URISyntaxException, IOException, InterruptedException {

        /**
         * spiderxmac:resources zoz$ cat /Users/zoz/zz/app/github/zozospider/note-hadoop-video1/src/main/resources/hdfs-site.xml
         * <configuration>
         *
         *     <property>
         *       <name>dfs.replication</name>
         *       <value>2</value>
         *       <description>
         *         Default block replication. The actual number of replications can be specified when the file is created. The default is used if replication is not specified in create time.
         *       </description>
         *     </property>
         *
         * </configuration>
         *
         * spiderxmac:tmp zoz$ ls /Users/zoz/zz/other/tmp/f2
         * /Users/zoz/zz/other/tmp/f2
         * spiderxmac:tmp zoz$
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_a/f2
         * ls: `/d2/d2_a/f2': No such file or directory
         * [zozo@vm017 hadoop-2.7.2]$
         */

        // 1. 获取 HDFS 客户端对象
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");

        // 通过 `客户端配置文件` 配置参数
        // 见 resources/hdfs-site.xml

        FileSystem fs = FileSystem.get(new URI("hdfs://193.112.38.200:9000"), conf, "zozo");

        // 2. 执行上传
        fs.copyFromLocalFile(new Path("/Users/zoz/zz/other/tmp/f2"), new Path("/d2/d2_a/f2"));

        // 3. 关闭资源
        fs.close();

        /**
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_a/f2
         * -rw-r--r--   2 zozo supergroup          8 2019-09-28 18:03 /d2/d2_a/f2
         * [zozo@vm017 hadoop-2.7.2]$
         */
    }

    /**
     * 上传本地文件到 HDFS (参数优先级: 客户端代码)
     * 优先级: 客户端代码 > 客户端配置文件 > 服务端配置文件 > 服务端默认配置
     */
    private static void copyFromLocalFileWithArgs2() throws URISyntaxException, IOException, InterruptedException {

        /**
         * spiderxmac:resources zoz$ cat /Users/zoz/zz/app/github/zozospider/note-hadoop-video1/src/main/resources/hdfs-site.xml
         * <configuration>
         *
         *     <property>
         *       <name>dfs.replication</name>
         *       <value>2</value>
         *       <description>
         *         Default block replication. The actual number of replications can be specified when the file is created. The default is used if replication is not specified in create time.
         *       </description>
         *     </property>
         *
         * </configuration>
         *
         * spiderxmac:tmp zoz$ ls /Users/zoz/zz/other/tmp/f3
         * /Users/zoz/zz/other/tmp/f3
         * spiderxmac:tmp zoz$
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_a/f3
         * ls: `/d2/d2_a/f3': No such file or directory
         * [zozo@vm017 hadoop-2.7.2]$
         */

        // 1. 获取 HDFS 客户端对象
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");

        // 通过 `客户端代码` 配置参数
        conf.set("dfs.replication", "1");

        FileSystem fs = FileSystem.get(new URI("hdfs://193.112.38.200:9000"), conf, "zozo");

        // 2. 执行上传
        fs.copyFromLocalFile(new Path("/Users/zoz/zz/other/tmp/f3"), new Path("/d2/d2_a/f3"));

        // 3. 关闭资源
        fs.close();

        /**
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_a/f3
         * -rw-r--r--   1 zozo supergroup          8 2019-09-28 18:07 /d2/d2_a/f3
         * [zozo@vm017 hadoop-2.7.2]$
         */
    }

    /**
     * 从 HDFS 拷贝 / 剪切文件到本地
     */
    private static void copyToLocalFile() throws URISyntaxException, IOException, InterruptedException {

        /**
         * spiderxmac:tmp zoz$ ls /Users/zoz/zz/other/tmp/f1_fromHDFS
         * ls: /Users/zoz/zz/other/tmp/f1_fromHDFS: No such file or directory
         * spiderxmac:tmp zoz$
         */

        // 1. 获取 HDFS 客户端对象
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(new URI("hdfs://193.112.38.200:9000"), conf, "zozo");

        // 2. 执行拷贝

        // delSrc: whether to delete the src. (是否删除源路径: HDFS) (默认 false)
        // src: src path. (源路径: HDFS)
        // dst: dst path. (目标路径: 本地)
        // useRawLocalFileSystem: whether to use RawLocalFileSystem as local file system or not. (是否使用 RawLocalFileSystem 作为本地文件系统) (默认 false)
        fs.copyToLocalFile(new Path("/d2/d2_a/f1"), new Path("/Users/zoz/zz/other/tmp/f1_fromHDFS"));
        // fs.copyToLocalFile(false, new Path("/d2/d2_a/f1"), new Path("/Users/zoz/zz/other/tmp/f1_fromHDFS"));
        // fs.copyToLocalFile(false, new Path("/d2/d2_a/f1"), new Path("/Users/zoz/zz/other/tmp/f1_fromHDFS"), false);

        // 3. 关闭资源
        fs.close();

        /**
         * spiderxmac:tmp zoz$ ls -l /Users/zoz/zz/other/tmp/f1_fromHDFS
         * -rw-r--r--  1 zoz  staff  8  9 28 18:18 /Users/zoz/zz/other/tmp/f1_fromHDFS
         * spiderxmac:tmp zoz$
         */
    }

    /**
     * 删除 HDFS 的路径 (文件 / 文件夹)
     */
    private static void delete() throws URISyntaxException, IOException, InterruptedException {

        /**
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_a/f3
         * -rw-r--r--   1 zozo supergroup          8 2019-09-28 18:07 /d2/d2_a/f3
         * [zozo@vm017 hadoop-2.7.2]$
         *
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_b
         * [zozo@vm017 hadoop-2.7.2]$
         *
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_c/
         * Found 2 items
         * -rw-r--r--   3 zozo supergroup          8 2019-09-28 20:19 /d2/d2_c/f1
         * -rw-r--r--   3 zozo supergroup          8 2019-09-28 20:20 /d2/d2_c/f2
         * [zozo@vm017 hadoop-2.7.2]$
         *
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_c/
         * Found 2 items
         * -rw-r--r--   3 zozo supergroup          8 2019-09-28 20:19 /d2/d2_c/f1
         * -rw-r--r--   3 zozo supergroup          8 2019-09-28 20:20 /d2/d2_c/f2
         * [zozo@vm017 hadoop-2.7.2]$
         */

        // 1. 获取 HDFS 客户端对象
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(new URI("hdfs://193.112.38.200:9000"), conf, "zozo");

        // 2. 删除 HDFS 的路径 (文件 / 文件夹)

        // f: the path to delete.
        // recursive: if path is a directory and set to true, the directory is deleted else throws an exception. In case of a file the recursive can be set to either true or false.
        // recursive: 是否递归删除 (
        // - 为 false 时只能删除空文件夹, 否则会抛出异常.
        // - 为 true 时可递归删除子目录.
        // boolean isDeleted = fs.delete(new Path("/d2/d2_a/f3"), false);
        // boolean isDeleted = fs.delete(new Path("/d2/d2_b"), false);
        // boolean isDeleted = fs.delete(new Path("/d2/d2_c"), false);
        boolean isDeleted = fs.delete(new Path("/d2/d2_c"), true);

        // 3. 关闭资源
        fs.close();

        System.out.println("isDeleted: " + isDeleted);
        /**
         * isDeleted: true
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_a/f3
         * ls: `/d2/d2_a/f3': No such file or directory
         * [zozo@vm017 hadoop-2.7.2]$
         *
         *
         * isDeleted: true
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_b
         * ls: `/d2/d2_b': No such file or directory
         * [zozo@vm017 hadoop-2.7.2]$
         *
         *
         * Exception in thread "main" org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.fs.PathIsNotEmptyDirectoryException): `/d2/d2_c is non empty': Directory is not empty
         * 	at org.apache.hadoop.hdfs.server.namenode.FSDirDeleteOp.delete(FSDirDeleteOp.java:85)
         * 	at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.delete(FSNamesystem.java:3712)
         * 	at org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer.delete(NameNodeRpcServer.java:952)
         * 	...
         *
         *
         * isDeleted: true
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_c/
         * ls: `/d2/d2_c/': No such file or directory
         * [zozo@vm017 hadoop-2.7.2]$
         */
    }

    /**
     * 修改 HDFS 的路径 (文件 / 文件夹) 名称
     */
    private static void rename() throws URISyntaxException, IOException, InterruptedException {

        /**
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_a/f2
         * -rw-r--r--   2 zozo supergroup          8 2019-09-28 18:03 /d2/d2_a/f2
         * [zozo@vm017 hadoop-2.7.2]$
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_b
         * Found 1 items
         * -rw-r--r--   3 zozo supergroup          8 2019-09-28 18:49 /d2/d2_b/f1
         * [zozo@vm017 hadoop-2.7.2]$
         */

        // 1. 获取 HDFS 客户端对象
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(new URI("hdfs://193.112.38.200:9000"), conf, "zozo");

        // 2. 修改 HDFS 的路径名称
        boolean isRenamed = fs.rename(new Path("/d2/d2_a/f2"), new Path("/d2/d2_a/f2_rename"));
        // boolean isRenamed = fs.rename(new Path("/d2/d2_b"), new Path("/d2/d2_b_rename"));

        // 3. 关闭资源
        fs.close();

        System.out.println("isRenamed: " + isRenamed);
        /**
         * isRenamed: true
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_a/f2
         * ls: `/d2/d2_a/f2': No such file or directory
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_a/f2_rename
         * -rw-r--r--   2 zozo supergroup          8 2019-09-28 18:03 /d2/d2_a/f2_rename
         * [zozo@vm017 hadoop-2.7.2]$
         *
         *
         * isRenamed: true
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_b
         * ls: `/d2/d2_b': No such file or directory
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_b_rename
         * Found 1 items
         * -rw-r--r--   3 zozo supergroup          8 2019-09-28 18:49 /d2/d2_b_rename/f1
         * [zozo@vm017 hadoop-2.7.2]$
         */
    }

    /**
     * 查看文件详情 (不包括文件夹)
     */
    private static void listFiles() throws URISyntaxException, IOException, InterruptedException {

        /**
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls -R /
         * drwxr-xr-x   - zozo supergroup          0 2019-09-28 20:51 /d2
         * drwxr-xr-x   - zozo supergroup          0 2019-09-28 18:47 /d2/d2_a
         * -rw-r--r--   3 zozo supergroup          8 2019-09-28 17:29 /d2/d2_a/f1
         * -rw-r--r--   2 zozo supergroup          8 2019-09-28 18:03 /d2/d2_a/f2_rename
         * drwxr-xr-x   - zozo supergroup          0 2019-09-28 18:49 /d2/d2_b
         * -rw-r--r--   3 zozo supergroup          8 2019-09-28 18:49 /d2/d2_b/f1
         * drwxr-xr-x   - zozo supergroup          0 2019-09-28 21:28 /d3
         * -rw-r--r--   3 zozo supergroup  212046774 2019-09-25 20:04 /hadoop-2.7.2.tar.gz
         * -rw-r--r--   3 zozo supergroup         36 2019-09-25 20:04 /wc.input
         * [zozo@vm017 hadoop-2.7.2]$
         */

        // 1. 获取 HDFS 客户端对象
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(new URI("hdfs://193.112.38.200:9000"), conf, "zozo");

        // 2. 获取所有文件
        RemoteIterator<LocatedFileStatus> locatedfileStatuses = fs.listFiles(new Path("/"), true);

        // 遍历所有文件
        while (locatedfileStatuses.hasNext()) {

            LocatedFileStatus locatedFileStatus = locatedfileStatuses.next();

            // 文件信息
            System.out.println("name: " + locatedFileStatus.getPath().getName()
                    + ", len: " + locatedFileStatus.getLen()
                    + ", permission: " + locatedFileStatus.getPermission());

            // 文件块信息
            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            for (int i = 0; i < blockLocations.length; i++) {
                System.out.println("  block[" + (i + 1) + "]: "
                        + "names: " + Arrays.asList(blockLocations[i].getNames())
                        + ", length: " + blockLocations[i].getLength()
                        + ", hosts: " + Arrays.asList(blockLocations[i].getHosts()));
            }

            System.out.println("---");
        }

        // 3. 关闭资源
        fs.close();

        /**
         * name: f1, len: 8, permission: rw-r--r--
         *   block[1]: names: [172.16.0.6:50010, 172.16.0.17:50010, 172.16.0.3:50010], length: 8, hosts: [vm06, vm017, vm03]
         * ---
         * name: f2_rename, len: 8, permission: rw-r--r--
         *   block[1]: names: [172.16.0.3:50010, 172.16.0.17:50010], length: 8, hosts: [vm03, vm017]
         * ---
         * name: f1, len: 8, permission: rw-r--r--
         *   block[1]: names: [172.16.0.6:50010, 172.16.0.17:50010, 172.16.0.3:50010], length: 8, hosts: [vm06, vm017, vm03]
         * ---
         * name: hadoop-2.7.2.tar.gz, len: 212046774, permission: rw-r--r--
         *   block[1]: names: [172.16.0.6:50010, 172.16.0.17:50010, 172.16.0.3:50010], length: 134217728, hosts: [vm06, vm017, vm03]
         *   block[2]: names: [172.16.0.17:50010, 172.16.0.3:50010, 172.16.0.6:50010], length: 77829046, hosts: [vm017, vm03, vm06]
         * ---
         * name: wc.input, len: 36, permission: rw-r--r--
         *   block[1]: names: [172.16.0.3:50010, 172.16.0.17:50010, 172.16.0.6:50010], length: 36, hosts: [vm03, vm017, vm06]
         * ---
         */
    }

    /**
     * 查看文件状态 (不支持递归)
     */
    private static void listStatus() throws URISyntaxException, IOException, InterruptedException {

        /**
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls -R /
         * drwxr-xr-x   - zozo supergroup          0 2019-09-28 20:51 /d2
         * drwxr-xr-x   - zozo supergroup          0 2019-09-28 18:47 /d2/d2_a
         * -rw-r--r--   3 zozo supergroup          8 2019-09-28 17:29 /d2/d2_a/f1
         * -rw-r--r--   2 zozo supergroup          8 2019-09-28 18:03 /d2/d2_a/f2_rename
         * drwxr-xr-x   - zozo supergroup          0 2019-09-28 18:49 /d2/d2_b
         * -rw-r--r--   3 zozo supergroup          8 2019-09-28 18:49 /d2/d2_b/f1
         * drwxr-xr-x   - zozo supergroup          0 2019-09-28 21:28 /d3
         * -rw-r--r--   3 zozo supergroup  212046774 2019-09-25 20:04 /hadoop-2.7.2.tar.gz
         * -rw-r--r--   3 zozo supergroup         36 2019-09-25 20:04 /wc.input
         * [zozo@vm017 hadoop-2.7.2]$
         */

        // 1. 获取 HDFS 客户端对象
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(new URI("hdfs://193.112.38.200:9000"), conf, "zozo");

        // 2. 获取所有文件状态
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        // FileStatus[] fileStatuses = fs.listStatus(new Path[]{new Path("/"), new Path("/d2/d2_a")});

        for (FileStatus fileStatus : fileStatuses) {

            if (fileStatus.isFile()) {
                System.out.println("file name: " + fileStatus.getPath().getName());
            }

            if (fileStatus.isDirectory()) {
                System.out.println("directory name: " + fileStatus.getPath().getName());
            }

        }

        // 3. 关闭资源
        fs.close();

        /**
         * directory name: d2
         * directory name: d3
         * file name: hadoop-2.7.2.tar.gz
         * file name: wc.input
         *
         *
         * directory name: d2
         * directory name: d3
         * file name: hadoop-2.7.2.tar.gz
         * file name: wc.input
         * file name: f1
         * file name: f2_rename
         */
    }

    /**
     * IO 流操作: 上传本地文件到 HDFS
     */
    private static void ioPutFileToHDFS() throws URISyntaxException, IOException, InterruptedException {

        /**
         * spiderxmac:tmp zoz$ cat /Users/zoz/zz/other/tmp/f3
         * I am f3
         * spiderxmac:tmp zoz$
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /d2/d2_c/f3
         * ls: `/d2/d2_c/f3': No such file or directory
         * [zozo@vm017 hadoop-2.7.2]$
         */

        // 1. 获取 HDFS 客户端对象
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(new URI("hdfs://193.112.38.200:9000"), conf, "zozo");

        // 2. 从本地获取输入流
        FileInputStream inputFromLocal = new FileInputStream(new File("/Users/zoz/zz/other/tmp/f3"));

        // 3. 从 HDFS 获取输出流
        FSDataOutputStream outputFromHDFS = fs.create(new Path("/d2/d2_c/f3"));

        // 4. 拷贝流
        IOUtils.copyBytes(inputFromLocal, outputFromHDFS, conf);

        // 5. 关闭资源
        IOUtils.closeStream(inputFromLocal);
        IOUtils.closeStream(outputFromHDFS);
        fs.close();

        /**
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -cat /d2/d2_c/f3
         * I am f3
         * [zozo@vm017 hadoop-2.7.2]$
         */
    }

    /**
     * IO 流操作: 从 HDFS 拷贝文件到本地
     */
    private static void ioGetFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {

        /**
         * spiderxmac:tmp zoz$ ls -l /Users/zoz/zz/other/tmp/f3_fromHDFS
         * -rw-r--r--  1 zoz  staff  8  9 28 22:24 /Users/zoz/zz/other/tmp/f3_fromHDFS
         * spiderxmac:tmp zoz$
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -cat /d2/d2_c/f3
         * I am f3
         * [zozo@vm017 hadoop-2.7.2]$
         */

        // 1. 获取 HDFS 客户端对象
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(new URI("hdfs://193.112.38.200:9000"), conf, "zozo");

        // 2. 从 HDFS 获取输入流
        FSDataInputStream inputFromHDFS = fs.open(new Path("/d2/d2_c/f3"));

        // 3. 从本地获取输出流
        FileOutputStream outputFromLocal = new FileOutputStream(new File("/Users/zoz/zz/other/tmp/f3_fromHDFS"));

        // 4. 拷贝流
        IOUtils.copyBytes(inputFromHDFS, outputFromLocal, conf);

        // 5. 关闭资源
        IOUtils.closeStream(inputFromHDFS);
        IOUtils.closeStream(outputFromLocal);
        fs.close();

        /**
         * spiderxmac:tmp zoz$ cat /Users/zoz/zz/other/tmp/f3_fromHDFS
         * I am f3
         * spiderxmac:tmp zoz$
         */
    }

    /**
     * IO 流操作: 从 HDFS 定位读取文件多个部分拷贝到本地
     */
    private static void ioSeekFileFromHDFS() throws InterruptedException, IOException, URISyntaxException {

        /**
         * spiderxmac:tmp zoz$ ls -l /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz
         * ls: /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz: No such file or directory
         * spiderxmac:tmp zoz$ ls -l /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part1
         * ls: /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part1: No such file or directory
         * spiderxmac:tmp zoz$ ls -l /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part2
         * ls: /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part2: No such file or directory
         * spiderxmac:tmp zoz$
         *
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -ls /hadoop-2.7.2.tar.gz
         * -rw-r--r--   3 zozo supergroup  212046774 2019-09-25 20:04 /hadoop-2.7.2.tar.gz
         * [zozo@vm017 hadoop-2.7.2]$ bin/hadoop fs -du -h /hadoop-2.7.2.tar.gz
         * 202.2 M  /hadoop-2.7.2.tar.gz
         * [zozo@vm017 hadoop-2.7.2]$
         */

        // 读取 HDFS 文件的前一部分
        // ioSeekFileFromHDFS1();
        // 读取 HDFS 文件的剩余部分
        // ioSeekFileFromHDFS2();

        /**
         * spiderxmac:tmp zoz$ ls -l /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part1
         * -rw-r--r--  1 zoz  staff  134217728  9 28 22:59 /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part1
         * spiderxmac:tmp zoz$ du -h /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part1
         * 128M	/Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part1
         * spiderxmac:tmp zoz$
         * spiderxmac:tmp zoz$ ls -l /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part2
         * -rw-r--r--  1 zoz  staff  77829046  9 28 23:07 /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part2
         * spiderxmac:tmp zoz$ du -h /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part2
         *  80M	/Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part2
         * spiderxmac:tmp zoz$
         * spiderxmac:tmp zoz$ cat hadoop-2.7.2.tar.gz.part1 >> hadoop-2.7.2.tar.gz
         * spiderxmac:tmp zoz$ cat hadoop-2.7.2.tar.gz.part2 >> hadoop-2.7.2.tar.gz
         * spiderxmac:tmp zoz$ ls -l hadoop-2.7.2.tar.gz
         * -rw-r--r--  1 zoz  staff  212046774  9 28 23:20 hadoop-2.7.2.tar.gz
         * spiderxmac:tmp zoz$ du -h hadoop-2.7.2.tar.gz
         * 208M	hadoop-2.7.2.tar.gz
         * spiderxmac:tmp zoz$
         */
    }

    /**
     * 读取 HDFS 文件的前一部分
     */
    private static void ioSeekFileFromHDFS1() throws URISyntaxException, IOException, InterruptedException {

        // 1. 获取 HDFS 客户端对象
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(new URI("hdfs://193.112.38.200:9000"), conf, "zozo");

        // 2. 从 HDFS 获取输入流
        FSDataInputStream inputFromHDFS = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3. 从本地获取输出流
        FileOutputStream outputFromLocal = new FileOutputStream(new File("/Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part1"));

        // 4. 拷贝流

        // 创建一个 1024B (1K) 到缓冲数组
        byte[] buf = new byte[1024];
        // 循环 128 * 1024 次, 每次拷贝 1K, 循环结束后, 即拷贝完 128 * 1024 * 1K = 128M 内容
        for (int i = 0; i < 128 * 1024; i++) {
            // 从 HDFS 输入流中读取 1K 内容到 buf 缓冲数组中
            inputFromHDFS.read(buf);
            // 将 buf 缓冲数组中的 1K 内容写入到本地输出流中
            outputFromLocal.write(buf);
        }

        // 5. 关闭资源
        IOUtils.closeStream(inputFromHDFS);
        IOUtils.closeStream(outputFromLocal);
        fs.close();

        System.out.println("over1");
        /**
         * spiderxmac:tmp zoz$ ls -l /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part1
         * -rw-r--r--  1 zoz  staff  134217728  9 28 22:59 /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part1
         * spiderxmac:tmp zoz$ du -h /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part1
         * 128M	/Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part1
         * spiderxmac:tmp zoz$
         */
    }

    /**
     * 读取 HDFS 文件的剩余部分
     */
    private static void ioSeekFileFromHDFS2() throws URISyntaxException, IOException, InterruptedException {

        // 1. 获取 HDFS 客户端对象
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(new URI("hdfs://193.112.38.200:9000"), conf, "zozo");

        // 2. 从 HDFS 获取输入流
        FSDataInputStream inputFromHDFS = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 查找输入流的指定位置作为起点开始读取 (从 128M 的位置开始读)
        inputFromHDFS.seek(128 * 1024 * 1024);

        // 3. 从本地获取输出流
        FileOutputStream outputFromLocal = new FileOutputStream(new File("/Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part2"));

        // 4. 拷贝流
        IOUtils.copyBytes(inputFromHDFS, outputFromLocal, conf);

        // 5. 关闭资源
        IOUtils.closeStream(inputFromHDFS);
        IOUtils.closeStream(outputFromLocal);
        fs.close();

        System.out.println("over2");
        /**
         * spiderxmac:tmp zoz$ ls -l /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part2
         * -rw-r--r--  1 zoz  staff  77829046  9 28 23:07 /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part2
         * spiderxmac:tmp zoz$ du -h /Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part2
         *  80M	/Users/zoz/zz/other/tmp/hadoop-2.7.2.tar.gz.part2
         * spiderxmac:tmp zoz$
         */
    }

}
