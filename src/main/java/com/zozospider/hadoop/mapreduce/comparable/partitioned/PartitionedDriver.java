package com.zozospider.hadoop.mapreduce.comparable.partitioned;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动: 分区排序 (按 field1 首字母 2 个分区, 且每个分区按 (field2 + field3) 倒叙排列)
 */
public class PartitionedDriver {

    /**
     * spiderxmac:input zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/input/Comparable/Partitioned/
     * total 8
     * -rw-r--r--  1 zoz  staff  99 10 11 23:23 f1
     * spiderxmac:input zoz$ cat Comparable/Partitioned/f1
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
     * spiderxmac:input zoz$
     * <p>
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/Comparable/Partitioned/
     * ls: /Users/zoz/zz/other/tmp/MapReduce/output/Comparable/Partitioned/: No such file or directory
     * spiderxmac:output zoz$
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/zoz/zz/other/tmp/MapReduce/input/Comparable/Partitioned", "/Users/zoz/zz/other/tmp/MapReduce/output/Comparable/Partitioned"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置 Partitioner 为 PartitionedPartitioner
        job.setPartitionerClass(PartitionedPartitioner.class);
        // 设置 ReduceTasks 个数为 2 (标准方式: 等于 PartitionedPartitioner 中定义的 partitions 个数)
        job.setNumReduceTasks(2);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(PartitionedDriver.class);
        job.setMapperClass(PartitionedMapper.class);
        job.setReducerClass(PartitionedReducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(PartitionedKeyWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(PartitionedKeyWritable.class);
        job.setOutputValueClass(Text.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/Comparable/Partitioned/
     * ls: /Users/zoz/zz/other/tmp/MapReduce/output/Comparable/Partitioned/: No such file or directory
     * spiderxmac:output zoz$
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/Comparable/Partitioned/
     * total 16
     * -rw-r--r--  1 zoz  staff    0 10 11 23:27 _SUCCESS
     * -rw-r--r--  1 zoz  staff  185 10 11 23:27 part-r-00000
     * -rw-r--r--  1 zoz  staff  433 10 11 23:27 part-r-00001
     * spiderxmac:output zoz$ cat Comparable/Partitioned/part-r-00000
     * PartitionedKeyWritable{field1=10, field2=15, fieldSum=30}	abc
     * PartitionedKeyWritable{field1=100, field2=3, fieldSum=6}	book
     * PartitionedKeyWritable{field1=99, field2=1, fieldSum=2}	good
     * spiderxmac:output zoz$ cat Comparable/Partitioned/part-r-00001
     * PartitionedKeyWritable{field1=1, field2=99, fieldSum=198}	ss
     * PartitionedKeyWritable{field1=1, field2=80, fieldSum=160}	what
     * PartitionedKeyWritable{field1=40, field2=25, fieldSum=50}	zoo
     * PartitionedKeyWritable{field1=50, field2=20, fieldSum=40}	xpp
     * PartitionedKeyWritable{field1=15, field2=10, fieldSum=20}	love
     * PartitionedKeyWritable{field1=30, field2=10, fieldSum=20}	qq
     * PartitionedKeyWritable{field1=100, field2=0, fieldSum=0}	zoo
     * spiderxmac:output zoz$
     */

    /**
     * 2019-10-11 23:27:42,385 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-11 23:27:42,745 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-11 23:27:42,746 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-11 23:27:43,292 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-11 23:27:43,304 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-11 23:27:43,321 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-11 23:27:43,426 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-11 23:27:43,576 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local1570919399_0001
     * 2019-10-11 23:27:43,884 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-11 23:27:43,888 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-11 23:27:43,889 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-11 23:27:43,892 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-11 23:27:43,893 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local1570919399_0001
     * 2019-10-11 23:27:43,915 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-11 23:27:43,916 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1570919399_0001_m_000000_0
     * 2019-10-11 23:27:43,938 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-11 23:27:43,943 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-11 23:27:43,943 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-11 23:27:43,946 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/zoz/zz/other/tmp/MapReduce/input/Comparable/Partitioned/f1:0+99
     * 2019-10-11 23:27:44,012 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-11 23:27:44,012 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-11 23:27:44,012 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-11 23:27:44,012 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-11 23:27:44,012 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-11 23:27:44,014 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-11 23:27:44,021 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-11 23:27:44,021 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-11 23:27:44,022 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-11 23:27:44,022 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 162; bufvoid = 104857600
     * 2019-10-11 23:27:44,022 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214360(104857440); length = 37/6553600
     * 2019-10-11 23:27:44,027 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-11 23:27:44,032 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1570919399_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-11 23:27:44,040 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-11 23:27:44,040 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1570919399_0001_m_000000_0' done.
     * 2019-10-11 23:27:44,040 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1570919399_0001_m_000000_0
     * 2019-10-11 23:27:44,040 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-11 23:27:44,043 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-11 23:27:44,043 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1570919399_0001_r_000000_0
     * 2019-10-11 23:27:44,050 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-11 23:27:44,050 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-11 23:27:44,050 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-11 23:27:44,053 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@6a54cf17
     * 2019-10-11 23:27:44,065 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=2672505600, maxSingleShuffleLimit=668126400, mergeThreshold=1763853824, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-11 23:27:44,067 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1570919399_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-11 23:27:44,094 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local1570919399_0001_m_000000_0 decomp: 58 len: 62 to MEMORY
     * 2019-10-11 23:27:44,107 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 58 bytes from map-output for attempt_local1570919399_0001_m_000000_0
     * 2019-10-11 23:27:44,109 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 58, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->58
     * 2019-10-11 23:27:44,109 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-11 23:27:44,110 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-11 23:27:44,110 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-11 23:27:44,115 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-11 23:27:44,115 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 44 bytes
     * 2019-10-11 23:27:44,116 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 58 bytes to disk to satisfy reduce memory limit
     * 2019-10-11 23:27:44,116 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 62 bytes from disk
     * 2019-10-11 23:27:44,116 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-11 23:27:44,116 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-11 23:27:44,117 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 44 bytes
     * 2019-10-11 23:27:44,117 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-11 23:27:44,136 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-11 23:27:44,141 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1570919399_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-11 23:27:44,142 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-11 23:27:44,142 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1570919399_0001_r_000000_0 is allowed to commit now
     * 2019-10-11 23:27:44,143 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1570919399_0001_r_000000_0' to file:/Users/zoz/zz/other/tmp/MapReduce/output/Comparable/Partitioned/_temporary/0/task_local1570919399_0001_r_000000
     * 2019-10-11 23:27:44,143 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-11 23:27:44,143 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1570919399_0001_r_000000_0' done.
     * 2019-10-11 23:27:44,144 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1570919399_0001_r_000000_0
     * 2019-10-11 23:27:44,144 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local1570919399_0001_r_000001_0
     * 2019-10-11 23:27:44,144 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-11 23:27:44,145 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-11 23:27:44,145 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-11 23:27:44,145 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@28e1c188
     * 2019-10-11 23:27:44,145 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=2672505600, maxSingleShuffleLimit=668126400, mergeThreshold=1763853824, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-11 23:27:44,146 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local1570919399_0001_r_000001_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-11 23:27:44,147 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#2 about to shuffle output of map attempt_local1570919399_0001_m_000000_0 decomp: 128 len: 132 to MEMORY
     * 2019-10-11 23:27:44,147 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 128 bytes from map-output for attempt_local1570919399_0001_m_000000_0
     * 2019-10-11 23:27:44,147 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 128, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->128
     * 2019-10-11 23:27:44,148 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-11 23:27:44,148 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-11 23:27:44,148 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-11 23:27:44,149 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-11 23:27:44,149 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 114 bytes
     * 2019-10-11 23:27:44,150 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 128 bytes to disk to satisfy reduce memory limit
     * 2019-10-11 23:27:44,150 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 132 bytes from disk
     * 2019-10-11 23:27:44,150 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-11 23:27:44,150 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-11 23:27:44,150 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 114 bytes
     * 2019-10-11 23:27:44,151 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-11 23:27:44,172 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local1570919399_0001_r_000001_0 is done. And is in the process of committing
     * 2019-10-11 23:27:44,173 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-11 23:27:44,173 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local1570919399_0001_r_000001_0 is allowed to commit now
     * 2019-10-11 23:27:44,173 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local1570919399_0001_r_000001_0' to file:/Users/zoz/zz/other/tmp/MapReduce/output/Comparable/Partitioned/_temporary/0/task_local1570919399_0001_r_000001
     * 2019-10-11 23:27:44,174 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-11 23:27:44,174 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local1570919399_0001_r_000001_0' done.
     * 2019-10-11 23:27:44,174 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local1570919399_0001_r_000001_0
     * 2019-10-11 23:27:44,174 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-11 23:27:44,897 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1570919399_0001 running in uber mode : false
     * 2019-10-11 23:27:44,898 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-11 23:27:44,899 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local1570919399_0001 completed successfully
     * 2019-10-11 23:27:44,908 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=1808
     * 		FILE: Number of bytes written=832752
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=10
     * 		Map output records=10
     * 		Map output bytes=162
     * 		Map output materialized bytes=194
     * 		Input split bytes=135
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=9
     * 		Reduce shuffle bytes=194
     * 		Reduce input records=10
     * 		Reduce output records=10
     * 		Spilled Records=20
     * 		Shuffled Maps =2
     * 		Failed Shuffles=0
     * 		Merged Map outputs=2
     * 		GC time elapsed (ms)=7
     * 		Total committed heap usage (bytes)=772276224
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=99
     * 	File Output Format Counters
     * 		Bytes Written=642
     *
     * Process finished with exit code 0
     */

}
