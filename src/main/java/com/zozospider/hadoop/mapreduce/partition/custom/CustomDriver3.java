package com.zozospider.hadoop.mapreduce.partition.custom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动 (错误方式)
 */
public class CustomDriver3 {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/partition/Custom3/
     * total 24
     * -rw-r--r--  1 user  staff    54B 10 10 20:36 f1
     * -rw-r--r--  1 user  staff    59B 10 10 20:36 f2
     * -rw-r--r--  1 user  staff    35B 10 10 20:36 f3
     * ➜  input cat partition/Custom3/f1
     * abc abc love
     * qq
     * love love qq
     * qq love
     * google book book
     * ➜  input cat partition/Custom3/f2
     * love love
     * qq google google
     * abc abc qq book
     * book book
     * do do
     * ➜  input cat partition/Custom3/f3
     * love qq
     * google abc
     * book book do do
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/partition/Custom3/
     * ls: /Users/user/other/tmp/MapReduce/output/partition/Custom3/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/partition/Custom3", "/Users/user/other/tmp/MapReduce/output/partition/Custom3"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Partitioner 为 CustomPartitioner
        job.setPartitionerClass(CustomPartitioner.class);
        // 设置 ReduceTasks 个数为 3 (错误方式: 小于 CustomPartitioner 中定义的 partitions 个数且不等于 1)
        job.setNumReduceTasks(3);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CustomDriver3.class);
        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/partition/Custom3/
     * ➜  output
     */

    /**
     * 2019-10-10 20:41:57,076 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 20:41:57,375 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 20:41:57,376 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 20:41:57,938 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 20:41:57,943 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 20:41:57,972 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 3
     * 2019-10-10 20:41:58,021 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:3
     * 2019-10-10 20:41:58,131 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1619192502_0001
     * 2019-10-10 20:41:58,390 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 20:41:58,391 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1619192502_0001
     * 2019-10-10 20:41:58,393 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 20:41:58,398 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:41:58,400 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 20:41:58,450 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 20:41:58,451 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1619192502_0001_m_000000_0
     * 2019-10-10 20:41:58,482 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:41:58,489 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:41:58,490 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:41:58,496 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Custom3/f2:0+59
     * 2019-10-10 20:41:58,566 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 20:41:58,566 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 20:41:58,566 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 20:41:58,566 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 20:41:58,566 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 20:41:58,568 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 20:41:58,575 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 20:41:58,575 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 20:41:58,575 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 18; bufvoid = 104857600
     * 2019-10-10 20:41:58,575 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214392(104857568); length = 5/6553600
     * 2019-10-10 20:41:58,581 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 20:41:58,584 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1619192502_0001_m_000001_0
     * 2019-10-10 20:41:58,586 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:41:58,586 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:41:58,586 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:41:58,588 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Custom3/f1:0+54
     * 2019-10-10 20:41:58,644 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 20:41:58,644 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 20:41:58,644 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 20:41:58,644 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 20:41:58,644 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 20:41:58,645 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 20:41:58,647 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 20:41:58,647 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 20:41:58,647 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 25; bufvoid = 104857600
     * 2019-10-10 20:41:58,647 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214388(104857552); length = 9/6553600
     * 2019-10-10 20:41:58,649 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 20:41:58,651 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1619192502_0001_m_000002_0
     * 2019-10-10 20:41:58,652 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:41:58,653 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:41:58,653 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:41:58,655 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Custom3/f3:0+35
     * 2019-10-10 20:41:58,711 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 20:41:58,711 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 20:41:58,711 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 20:41:58,711 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 20:41:58,711 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 20:41:58,712 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 20:41:58,713 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 20:41:58,713 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 20:41:58,714 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 9; bufvoid = 104857600
     * 2019-10-10 20:41:58,714 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214396(104857584); length = 1/6553600
     * 2019-10-10 20:41:58,717 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 20:41:58,719 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 20:41:58,721 WARN [org.apache.hadoop.mapred.LocalJobRunner] - job_local1619192502_0001
     * java.lang.Exception: java.io.IOException: Illegal partition for qq (3)
     * 	at org.apache.hadoop.mapred.LocalJobRunner$Job.runTasks(LocalJobRunner.java:462)
     * 	at org.apache.hadoop.mapred.LocalJobRunner$Job.run(LocalJobRunner.java:522)
     * Caused by: java.io.IOException: Illegal partition for qq (3)
     * 	at org.apache.hadoop.mapred.MapTask$MapOutputBuffer.collect(MapTask.java:1082)
     * 	at org.apache.hadoop.mapred.MapTask$NewOutputCollector.write(MapTask.java:715)
     * 	at org.apache.hadoop.mapreduce.task.TaskInputOutputContextImpl.write(TaskInputOutputContextImpl.java:89)
     * 	at org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context.write(WrappedMapper.java:112)
     * 	at com.zozospider.hadoop.mapreduce.partition.custom.CustomMapper.map(CustomMapper.java:41)
     * 	at com.zozospider.hadoop.mapreduce.partition.custom.CustomMapper.map(CustomMapper.java:13)
     * 	at org.apache.hadoop.mapreduce.Mapper.run(Mapper.java:146)
     * 	at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:787)
     * 	at org.apache.hadoop.mapred.MapTask.run(MapTask.java:341)
     * 	at org.apache.hadoop.mapred.LocalJobRunner$Job$MapTaskRunnable.run(LocalJobRunner.java:243)
     * 	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
     * 	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
     * 	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
     * 	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
     * 	at java.lang.Thread.run(Thread.java:748)
     * 2019-10-10 20:41:59,396 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1619192502_0001 running in uber mode : false
     * 2019-10-10 20:41:59,398 INFO [org.apache.hadoop.mapreduce.Job] -  map 0% reduce 0%
     * 2019-10-10 20:41:59,399 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1619192502_0001 failed with state FAILED due to: NA
     * 2019-10-10 20:41:59,402 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 0
     *
     * Process finished with exit code 1
     */

}
