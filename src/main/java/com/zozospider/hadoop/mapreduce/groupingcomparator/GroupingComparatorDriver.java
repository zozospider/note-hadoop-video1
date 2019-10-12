package com.zozospider.hadoop.mapreduce.groupingcomparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动: 找出 field1 相同的多行中 field2 最大的那 1 行
 */
public class GroupingComparatorDriver {

    /**
     * spiderxmac:input zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/input/GroupingComparator/
     * total 8
     * -rw-r--r--  1 zoz  staff  159 10 12 22:55 f1
     * spiderxmac:input zoz$ cat GroupingComparator/f1
     * one 1 100
     * two 2 500
     * five 5 500
     * one. 1 300
     * three 3 200
     * one.. 1 200
     * seven 7 800
     * four 4 800
     * six 6 700
     * five. 5 500
     * nine 9 200
     * nine. 9 300
     * eight 8 100
     * nine.. 9 900
     * spiderxmac:input zoz$
     * <p>
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/GroupingComparator/
     * ls: /Users/zoz/zz/other/tmp/MapReduce/output/GroupingComparator/: No such file or directory
     * spiderxmac:output zoz$
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/zoz/zz/other/tmp/MapReduce/input/GroupingComparator", "/Users/zoz/zz/other/tmp/MapReduce/output/GroupingComparator"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 GroupingComparator 为 GroupingComparatorKeyComparator
        job.setGroupingComparatorClass(GroupingComparatorKeyComparator.class);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(GroupingComparatorDriver.class);
        job.setMapperClass(GroupingComparatorMapper.class);
        job.setReducerClass(GroupingComparatorReducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(GroupingComparatorKeyWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(GroupingComparatorKeyWritable.class);
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
