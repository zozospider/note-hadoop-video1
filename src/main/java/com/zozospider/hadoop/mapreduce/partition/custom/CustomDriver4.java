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
 * MapReduce 驱动 (非标准方式)
 */
public class CustomDriver4 {

    /**
     * ➜  input ll /Users/user/other/tmp/MapReduce/input/partition/Custom4/
     * total 24
     * -rw-r--r--  1 user  staff    54B 10 10 20:43 f1
     * -rw-r--r--  1 user  staff    59B 10 10 20:43 f2
     * -rw-r--r--  1 user  staff    35B 10 10 20:43 f3
     * ➜  input cat partition/Custom4/f1
     * abc abc love
     * qq
     * love love qq
     * qq love
     * google book book
     * ➜  input cat partition/Custom4/f2
     * love love
     * qq google google
     * abc abc qq book
     * book book
     * do do
     * ➜  input cat partition/Custom4/f3
     * love qq
     * google abc
     * book book do do
     * ➜  input
     * <p>
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/partition/Custom4/
     * ls: /Users/user/other/tmp/MapReduce/output/partition/Custom4/: No such file or directory
     * ➜  output
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/user/other/tmp/MapReduce/input/partition/Custom4", "/Users/user/other/tmp/MapReduce/output/partition/Custom4"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Partitioner 为 CustomPartitioner
        job.setPartitionerClass(CustomPartitioner.class);
        // 设置 ReduceTasks 个数为 6 (非标准方式: 大于 CustomPartitioner 中定义的 partitions 个数)
        job.setNumReduceTasks(6);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(CustomDriver4.class);
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
     * ➜  output ll /Users/user/other/tmp/MapReduce/output/partition/Custom4/
     * total 24
     * -rw-r--r--  1 user  staff     0B 10 10 20:45 _SUCCESS
     * -rw-r--r--  1 user  staff    18B 10 10 20:45 part-r-00000
     * -rw-r--r--  1 user  staff    16B 10 10 20:45 part-r-00001
     * -rw-r--r--  1 user  staff     0B 10 10 20:45 part-r-00002
     * -rw-r--r--  1 user  staff     5B 10 10 20:45 part-r-00003
     * -rw-r--r--  1 user  staff     0B 10 10 20:45 part-r-00004
     * -rw-r--r--  1 user  staff     0B 10 10 20:45 part-r-00005
     * ➜  output cat partition/Custom4/part-r-00000
     * abc	5
     * book	7
     * do	4
     * ➜  output cat partition/Custom4/part-r-00001
     * google	4
     * love	7
     * ➜  output cat partition/Custom4/part-r-00002
     * ➜  output cat partition/Custom4/part-r-00003
     * qq	6
     * ➜  output cat partition/Custom4/part-r-00004
     * ➜  output cat partition/Custom4/part-r-00005
     * ➜  output
     */

    /**
     * 2019-10-10 20:45:22,161 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-10 20:45:22,441 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-10 20:45:22,442 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-10 20:45:23,063 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-10 20:45:23,068 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-10 20:45:23,097 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 3
     * 2019-10-10 20:45:23,147 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:3
     * 2019-10-10 20:45:23,270 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1855256012_0001
     * 2019-10-10 20:45:23,499 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-10 20:45:23,500 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-10 20:45:23,501 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1855256012_0001
     * 2019-10-10 20:45:23,504 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:45:23,506 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-10 20:45:23,555 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-10 20:45:23,556 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1855256012_0001_m_000000_0
     * 2019-10-10 20:45:23,588 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:45:23,595 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:45:23,596 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:45:23,602 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Custom4/f2:0+59
     * 2019-10-10 20:45:23,676 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 20:45:23,676 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 20:45:23,676 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 20:45:23,676 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 20:45:23,676 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 20:45:23,678 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 20:45:23,690 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 20:45:23,690 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 20:45:23,691 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 20:45:23,691 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 111; bufvoid = 104857600
     * 2019-10-10 20:45:23,691 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214348(104857392); length = 49/6553600
     * 2019-10-10 20:45:23,703 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 20:45:23,708 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1855256012_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-10 20:45:23,719 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 20:45:23,720 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1855256012_0001_m_000000_0' done.
     * 2019-10-10 20:45:23,720 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1855256012_0001_m_000000_0
     * 2019-10-10 20:45:23,720 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1855256012_0001_m_000001_0
     * 2019-10-10 20:45:23,721 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:45:23,722 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:45:23,722 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:45:23,723 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Custom4/f1:0+54
     * 2019-10-10 20:45:23,777 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 20:45:23,777 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 20:45:23,777 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 20:45:23,777 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 20:45:23,777 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 20:45:23,778 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 20:45:23,783 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 20:45:23,783 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 20:45:23,783 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 20:45:23,783 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 102; bufvoid = 104857600
     * 2019-10-10 20:45:23,783 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214352(104857408); length = 45/6553600
     * 2019-10-10 20:45:23,786 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 20:45:23,787 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1855256012_0001_m_000001_0 is done. And is in the process of committing
     * 2019-10-10 20:45:23,790 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 20:45:23,790 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1855256012_0001_m_000001_0' done.
     * 2019-10-10 20:45:23,790 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1855256012_0001_m_000001_0
     * 2019-10-10 20:45:23,790 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1855256012_0001_m_000002_0
     * 2019-10-10 20:45:23,791 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:45:23,792 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:45:23,792 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:45:23,794 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/user/other/tmp/MapReduce/input/partition/Custom4/f3:0+35
     * 2019-10-10 20:45:23,849 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-10 20:45:23,849 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-10 20:45:23,849 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-10 20:45:23,849 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-10 20:45:23,849 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-10 20:45:23,850 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-10 20:45:23,851 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-10 20:45:23,851 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-10 20:45:23,851 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-10 20:45:23,851 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 67; bufvoid = 104857600
     * 2019-10-10 20:45:23,851 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214368(104857472); length = 29/6553600
     * 2019-10-10 20:45:23,853 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-10 20:45:23,855 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1855256012_0001_m_000002_0 is done. And is in the process of committing
     * 2019-10-10 20:45:23,857 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-10 20:45:23,857 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1855256012_0001_m_000002_0' done.
     * 2019-10-10 20:45:23,857 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1855256012_0001_m_000002_0
     * 2019-10-10 20:45:23,858 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-10 20:45:23,864 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-10 20:45:23,864 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1855256012_0001_r_000000_0
     * 2019-10-10 20:45:23,871 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:45:23,872 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:45:23,872 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:45:23,875 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@6302ecd6
     * 2019-10-10 20:45:23,887 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 20:45:23,889 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1855256012_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 20:45:23,927 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1855256012_0001_m_000002_0 decomp: 52 len: 56 to MEMORY
     * 2019-10-10 20:45:23,939 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 52 bytes from map-output for attempt_local1855256012_0001_m_000002_0
     * 2019-10-10 20:45:23,941 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 52, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->52
     * 2019-10-10 20:45:23,945 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1855256012_0001_m_000001_0 decomp: 44 len: 48 to MEMORY
     * 2019-10-10 20:45:23,945 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 44 bytes from map-output for attempt_local1855256012_0001_m_000001_0
     * 2019-10-10 20:45:23,945 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 44, inMemoryMapOutputs.size() -> 2, commitMemory -> 52, usedMemory ->96
     * 2019-10-10 20:45:23,947 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1855256012_0001_m_000000_0 decomp: 73 len: 77 to MEMORY
     * 2019-10-10 20:45:23,948 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 73 bytes from map-output for attempt_local1855256012_0001_m_000000_0
     * 2019-10-10 20:45:23,949 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 73, inMemoryMapOutputs.size() -> 3, commitMemory -> 96, usedMemory ->169
     * 2019-10-10 20:45:23,949 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 20:45:23,950 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:23,962 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 20:45:23,971 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 20:45:23,971 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 3 segments left of total size: 151 bytes
     * 2019-10-10 20:45:23,972 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 169 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 20:45:23,973 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 169 bytes from disk
     * 2019-10-10 20:45:23,974 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 20:45:23,974 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 20:45:23,974 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 159 bytes
     * 2019-10-10 20:45:23,975 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:23,991 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-10 20:45:23,999 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1855256012_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-10 20:45:24,001 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,001 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1855256012_0001_r_000000_0 is allowed to commit now
     * 2019-10-10 20:45:24,002 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1855256012_0001_r_000000_0' to file:/Users/user/other/tmp/MapReduce/output/partition/Custom4/_temporary/0/task_local1855256012_0001_r_000000
     * 2019-10-10 20:45:24,003 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 20:45:24,003 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1855256012_0001_r_000000_0' done.
     * 2019-10-10 20:45:24,003 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1855256012_0001_r_000000_0
     * 2019-10-10 20:45:24,003 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1855256012_0001_r_000001_0
     * 2019-10-10 20:45:24,005 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:45:24,005 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:45:24,005 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:45:24,005 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@6ec056cb
     * 2019-10-10 20:45:24,006 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 20:45:24,007 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1855256012_0001_r_000001_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 20:45:24,009 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#2 about to shuffle output of map attempt_local1855256012_0001_m_000002_0 decomp: 26 len: 30 to MEMORY
     * 2019-10-10 20:45:24,009 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 26 bytes from map-output for attempt_local1855256012_0001_m_000002_0
     * 2019-10-10 20:45:24,009 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 26, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->26
     * 2019-10-10 20:45:24,011 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#2 about to shuffle output of map attempt_local1855256012_0001_m_000001_0 decomp: 59 len: 63 to MEMORY
     * 2019-10-10 20:45:24,012 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 59 bytes from map-output for attempt_local1855256012_0001_m_000001_0
     * 2019-10-10 20:45:24,012 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 59, inMemoryMapOutputs.size() -> 2, commitMemory -> 26, usedMemory ->85
     * 2019-10-10 20:45:24,013 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#2 about to shuffle output of map attempt_local1855256012_0001_m_000000_0 decomp: 50 len: 54 to MEMORY
     * 2019-10-10 20:45:24,014 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 50 bytes from map-output for attempt_local1855256012_0001_m_000000_0
     * 2019-10-10 20:45:24,014 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 50, inMemoryMapOutputs.size() -> 3, commitMemory -> 85, usedMemory ->135
     * 2019-10-10 20:45:24,015 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 20:45:24,015 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,016 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 20:45:24,017 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 20:45:24,017 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 3 segments left of total size: 108 bytes
     * 2019-10-10 20:45:24,018 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 135 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 20:45:24,018 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 135 bytes from disk
     * 2019-10-10 20:45:24,019 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 20:45:24,019 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 20:45:24,019 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 122 bytes
     * 2019-10-10 20:45:24,020 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,034 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1855256012_0001_r_000001_0 is done. And is in the process of committing
     * 2019-10-10 20:45:24,035 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,035 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1855256012_0001_r_000001_0 is allowed to commit now
     * 2019-10-10 20:45:24,036 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1855256012_0001_r_000001_0' to file:/Users/user/other/tmp/MapReduce/output/partition/Custom4/_temporary/0/task_local1855256012_0001_r_000001
     * 2019-10-10 20:45:24,037 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 20:45:24,037 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1855256012_0001_r_000001_0' done.
     * 2019-10-10 20:45:24,037 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1855256012_0001_r_000001_0
     * 2019-10-10 20:45:24,037 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1855256012_0001_r_000002_0
     * 2019-10-10 20:45:24,038 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:45:24,039 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:45:24,039 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:45:24,039 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@43305897
     * 2019-10-10 20:45:24,039 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 20:45:24,040 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1855256012_0001_r_000002_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 20:45:24,042 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#3 about to shuffle output of map attempt_local1855256012_0001_m_000002_0 decomp: 2 len: 6 to MEMORY
     * 2019-10-10 20:45:24,042 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 2 bytes from map-output for attempt_local1855256012_0001_m_000002_0
     * 2019-10-10 20:45:24,042 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 2, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->2
     * 2019-10-10 20:45:24,044 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#3 about to shuffle output of map attempt_local1855256012_0001_m_000001_0 decomp: 2 len: 6 to MEMORY
     * 2019-10-10 20:45:24,044 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 2 bytes from map-output for attempt_local1855256012_0001_m_000001_0
     * 2019-10-10 20:45:24,044 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 2, inMemoryMapOutputs.size() -> 2, commitMemory -> 2, usedMemory ->4
     * 2019-10-10 20:45:24,046 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#3 about to shuffle output of map attempt_local1855256012_0001_m_000000_0 decomp: 2 len: 6 to MEMORY
     * 2019-10-10 20:45:24,046 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 2 bytes from map-output for attempt_local1855256012_0001_m_000000_0
     * 2019-10-10 20:45:24,046 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 2, inMemoryMapOutputs.size() -> 3, commitMemory -> 4, usedMemory ->6
     * 2019-10-10 20:45:24,047 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 20:45:24,047 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,048 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 20:45:24,049 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 20:45:24,049 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 0 segments left of total size: 0 bytes
     * 2019-10-10 20:45:24,050 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 6 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 20:45:24,050 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 6 bytes from disk
     * 2019-10-10 20:45:24,050 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 20:45:24,050 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 20:45:24,051 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 0 segments left of total size: 0 bytes
     * 2019-10-10 20:45:24,051 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,062 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1855256012_0001_r_000002_0 is done. And is in the process of committing
     * 2019-10-10 20:45:24,064 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,064 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1855256012_0001_r_000002_0 is allowed to commit now
     * 2019-10-10 20:45:24,065 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1855256012_0001_r_000002_0' to file:/Users/user/other/tmp/MapReduce/output/partition/Custom4/_temporary/0/task_local1855256012_0001_r_000002
     * 2019-10-10 20:45:24,066 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 20:45:24,066 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1855256012_0001_r_000002_0' done.
     * 2019-10-10 20:45:24,066 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1855256012_0001_r_000002_0
     * 2019-10-10 20:45:24,066 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1855256012_0001_r_000003_0
     * 2019-10-10 20:45:24,068 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:45:24,068 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:45:24,068 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:45:24,068 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@6c10721f
     * 2019-10-10 20:45:24,069 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 20:45:24,070 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1855256012_0001_r_000003_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 20:45:24,071 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#4 about to shuffle output of map attempt_local1855256012_0001_m_000002_0 decomp: 11 len: 15 to MEMORY
     * 2019-10-10 20:45:24,072 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 11 bytes from map-output for attempt_local1855256012_0001_m_000002_0
     * 2019-10-10 20:45:24,072 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 11, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->11
     * 2019-10-10 20:45:24,073 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#4 about to shuffle output of map attempt_local1855256012_0001_m_000001_0 decomp: 29 len: 33 to MEMORY
     * 2019-10-10 20:45:24,074 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 29 bytes from map-output for attempt_local1855256012_0001_m_000001_0
     * 2019-10-10 20:45:24,074 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 29, inMemoryMapOutputs.size() -> 2, commitMemory -> 11, usedMemory ->40
     * 2019-10-10 20:45:24,075 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#4 about to shuffle output of map attempt_local1855256012_0001_m_000000_0 decomp: 20 len: 24 to MEMORY
     * 2019-10-10 20:45:24,076 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 20 bytes from map-output for attempt_local1855256012_0001_m_000000_0
     * 2019-10-10 20:45:24,076 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 20, inMemoryMapOutputs.size() -> 3, commitMemory -> 40, usedMemory ->60
     * 2019-10-10 20:45:24,076 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 20:45:24,077 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,077 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 20:45:24,078 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 20:45:24,078 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 3 segments left of total size: 45 bytes
     * 2019-10-10 20:45:24,078 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 60 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 20:45:24,079 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 60 bytes from disk
     * 2019-10-10 20:45:24,079 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 20:45:24,079 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 20:45:24,079 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 51 bytes
     * 2019-10-10 20:45:24,080 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,090 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1855256012_0001_r_000003_0 is done. And is in the process of committing
     * 2019-10-10 20:45:24,091 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,091 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1855256012_0001_r_000003_0 is allowed to commit now
     * 2019-10-10 20:45:24,092 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1855256012_0001_r_000003_0' to file:/Users/user/other/tmp/MapReduce/output/partition/Custom4/_temporary/0/task_local1855256012_0001_r_000003
     * 2019-10-10 20:45:24,092 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 20:45:24,092 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1855256012_0001_r_000003_0' done.
     * 2019-10-10 20:45:24,092 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1855256012_0001_r_000003_0
     * 2019-10-10 20:45:24,092 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1855256012_0001_r_000004_0
     * 2019-10-10 20:45:24,093 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:45:24,094 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:45:24,094 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:45:24,094 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@fc44594
     * 2019-10-10 20:45:24,094 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 20:45:24,095 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1855256012_0001_r_000004_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 20:45:24,096 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#5 about to shuffle output of map attempt_local1855256012_0001_m_000002_0 decomp: 2 len: 6 to MEMORY
     * 2019-10-10 20:45:24,097 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 2 bytes from map-output for attempt_local1855256012_0001_m_000002_0
     * 2019-10-10 20:45:24,097 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 2, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->2
     * 2019-10-10 20:45:24,099 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#5 about to shuffle output of map attempt_local1855256012_0001_m_000001_0 decomp: 2 len: 6 to MEMORY
     * 2019-10-10 20:45:24,099 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 2 bytes from map-output for attempt_local1855256012_0001_m_000001_0
     * 2019-10-10 20:45:24,099 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 2, inMemoryMapOutputs.size() -> 2, commitMemory -> 2, usedMemory ->4
     * 2019-10-10 20:45:24,100 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#5 about to shuffle output of map attempt_local1855256012_0001_m_000000_0 decomp: 2 len: 6 to MEMORY
     * 2019-10-10 20:45:24,101 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 2 bytes from map-output for attempt_local1855256012_0001_m_000000_0
     * 2019-10-10 20:45:24,101 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 2, inMemoryMapOutputs.size() -> 3, commitMemory -> 4, usedMemory ->6
     * 2019-10-10 20:45:24,101 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 20:45:24,102 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,102 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 20:45:24,103 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 20:45:24,103 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 0 segments left of total size: 0 bytes
     * 2019-10-10 20:45:24,104 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 6 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 20:45:24,104 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 6 bytes from disk
     * 2019-10-10 20:45:24,104 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 20:45:24,104 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 20:45:24,105 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 0 segments left of total size: 0 bytes
     * 2019-10-10 20:45:24,105 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,115 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1855256012_0001_r_000004_0 is done. And is in the process of committing
     * 2019-10-10 20:45:24,116 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,116 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1855256012_0001_r_000004_0 is allowed to commit now
     * 2019-10-10 20:45:24,116 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1855256012_0001_r_000004_0' to file:/Users/user/other/tmp/MapReduce/output/partition/Custom4/_temporary/0/task_local1855256012_0001_r_000004
     * 2019-10-10 20:45:24,117 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 20:45:24,117 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1855256012_0001_r_000004_0' done.
     * 2019-10-10 20:45:24,117 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1855256012_0001_r_000004_0
     * 2019-10-10 20:45:24,117 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1855256012_0001_r_000005_0
     * 2019-10-10 20:45:24,118 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-10 20:45:24,118 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-10 20:45:24,118 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-10 20:45:24,118 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@30274b99
     * 2019-10-10 20:45:24,118 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=1336252800, maxSingleShuffleLimit=334063200, mergeThreshold=881926912, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-10 20:45:24,119 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1855256012_0001_r_000005_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-10 20:45:24,120 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#6 about to shuffle output of map attempt_local1855256012_0001_m_000002_0 decomp: 2 len: 6 to MEMORY
     * 2019-10-10 20:45:24,121 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 2 bytes from map-output for attempt_local1855256012_0001_m_000002_0
     * 2019-10-10 20:45:24,121 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 2, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->2
     * 2019-10-10 20:45:24,122 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#6 about to shuffle output of map attempt_local1855256012_0001_m_000001_0 decomp: 2 len: 6 to MEMORY
     * 2019-10-10 20:45:24,122 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 2 bytes from map-output for attempt_local1855256012_0001_m_000001_0
     * 2019-10-10 20:45:24,122 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 2, inMemoryMapOutputs.size() -> 2, commitMemory -> 2, usedMemory ->4
     * 2019-10-10 20:45:24,123 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#6 about to shuffle output of map attempt_local1855256012_0001_m_000000_0 decomp: 2 len: 6 to MEMORY
     * 2019-10-10 20:45:24,123 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 2 bytes from map-output for attempt_local1855256012_0001_m_000000_0
     * 2019-10-10 20:45:24,123 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 2, inMemoryMapOutputs.size() -> 3, commitMemory -> 4, usedMemory ->6
     * 2019-10-10 20:45:24,123 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-10 20:45:24,124 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,124 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 3 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-10 20:45:24,125 INFO [org.apache.hadoop.mapred.Merger] - Merging 3 sorted segments
     * 2019-10-10 20:45:24,125 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 0 segments left of total size: 0 bytes
     * 2019-10-10 20:45:24,125 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 3 segments, 6 bytes to disk to satisfy reduce memory limit
     * 2019-10-10 20:45:24,125 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 6 bytes from disk
     * 2019-10-10 20:45:24,125 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-10 20:45:24,125 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-10 20:45:24,126 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 0 segments left of total size: 0 bytes
     * 2019-10-10 20:45:24,126 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,136 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1855256012_0001_r_000005_0 is done. And is in the process of committing
     * 2019-10-10 20:45:24,137 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 3 / 3 copied.
     * 2019-10-10 20:45:24,138 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1855256012_0001_r_000005_0 is allowed to commit now
     * 2019-10-10 20:45:24,138 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1855256012_0001_r_000005_0' to file:/Users/user/other/tmp/MapReduce/output/partition/Custom4/_temporary/0/task_local1855256012_0001_r_000005
     * 2019-10-10 20:45:24,139 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-10 20:45:24,139 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1855256012_0001_r_000005_0' done.
     * 2019-10-10 20:45:24,139 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1855256012_0001_r_000005_0
     * 2019-10-10 20:45:24,139 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-10 20:45:24,507 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1855256012_0001 running in uber mode : false
     * 2019-10-10 20:45:24,508 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-10 20:45:24,509 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1855256012_0001 completed successfully
     * 2019-10-10 20:45:24,526 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=27950
     * 		FILE: Number of bytes written=2515803
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=13
     * 		Map output records=33
     * 		Map output bytes=280
     * 		Map output materialized bytes=454
     * 		Input split bytes=384
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=6
     * 		Reduce shuffle bytes=454
     * 		Reduce input records=33
     * 		Reduce output records=6
     * 		Spilled Records=66
     * 		Shuffled Maps =18
     * 		Failed Shuffles=0
     * 		Merged Map outputs=18
     * 		GC time elapsed (ms)=10
     * 		Total committed heap usage (bytes)=3689938944
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=148
     * 	File Output Format Counters
     * 		Bytes Written=99
     *
     * Process finished with exit code 0
     */

}
