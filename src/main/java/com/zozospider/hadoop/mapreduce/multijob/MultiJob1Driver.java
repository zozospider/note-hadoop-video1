package com.zozospider.hadoop.mapreduce.multijob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * MapReduce 驱动:
 */
public class MultiJob1Driver {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/MultiJob/one/
     * total 24
     * -rw-r--r--  1 user  staff    20B 10 25 21:31 f1
     * -rw-r--r--  1 user  staff    24B 10 25 21:32 f2
     * -rw-r--r--  1 user  staff    12B 10 25 21:31 f3
     * ➜  input cat MultiJob/one/f1
     * abc zoo
     * zoo why
     * abc
     * ➜  input cat MultiJob/one/f2
     * why zoo zoo why
     * abc why
     * ➜  input cat MultiJob/one/f3
     * zoo why
     * why
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/MultiJob/one/
     * ls: /Users/user/other/tmp/MapReduce/output/MultiJob/one/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/MultiJob/one", "/Users/user/other/tmp/MapReduce/output/MultiJob/one"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


    }


    /**
     *
     */

    /**
     *
     */

}
