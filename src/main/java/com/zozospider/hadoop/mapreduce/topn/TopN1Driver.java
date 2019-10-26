package com.zozospider.hadoop.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动:
 */
public class TopN1Driver {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/TopN1/
     * total 8
     * -rw-r--r--  1 user  staff    99B 10 26 19:11 f1
     * ➜  input cat TopN1/f1
     * abc 10 15
     * qq 30 10
     * xpp 50 20
     * book 100 3
     * good 99 1
     * love 15 10
     * ss 1 99
     * zoo 40 25
     * zoo 100 0
     * what 1 80
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/TopN1/
     * ls: /Users/user/other/tmp/MapReduce/output/TopN1/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/TopN1", "/Users/user/other/tmp/MapReduce/output/TopN1"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(TopN1Driver.class);
        job.setMapperClass(TopN1Mapper.class);
        job.setReducerClass(TopN1Reducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(TopN1KeyWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(TopN1KeyWritable.class);
        job.setOutputValueClass(Text.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     *
     */

    /**
     *
     */

}
